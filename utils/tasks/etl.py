"""Module for ETL task creation and execution."""

import logging
from pathlib import Path
from typing import Callable, Optional, Any

import pandas as pd
from airflow.sdk import task, XComArg


from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.factory import create_default_s3_handler, create_local_handler
from infra.database.factory import create_db_handler
from infra.catalog.iceberg import generate_catalog_properties, IcebergCatalog
from enums.filesystem import IcebergTableStatus
from utils.tasks.s3 import write_to_s3
from utils.control.structures import remove_grist_internal_cols
from utils.dataframe import df_info
from utils.config.tasks import (
    get_source_grist,
    get_selector_info,
    column_mapping_dataframe,
    column_mapping_dict,
    get_source_fichier,
)
from utils.config.dag_params import get_db_info, get_execution_date, get_project_name
from _types.dags import ETLStep, TaskConfig
from enums.database import DatabaseType
from utils.config.vars import DEFAULT_POLARIS_HOST


def _add_import_metadata(df: pd.DataFrame, context: dict) -> pd.DataFrame:
    """Add import timestamp and date columns."""
    execution_date = get_execution_date(context=context)

    dt_no_timezone = execution_date.replace(tzinfo=None)
    df["import_timestamp"] = dt_no_timezone
    df["import_date"] = dt_no_timezone.date()
    return df


def _add_snapshot_id_metadata(df: pd.DataFrame, context: dict) -> pd.DataFrame:
    """Add snapshot_id column."""
    snapshot_id = context["ti"].xcom_pull(
        key="snapshot_id", task_ids="get_projet_snapshot"
    )
    if not snapshot_id:
        raise ValueError("snapshot_id is not defined")

    df["snapshot_id"] = snapshot_id
    return df


def create_grist_etl_task(
    selecteur: str,
    doc_selecteur: Optional[str] = None,
    normalisation_process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    version: str = "v1",
) -> Callable[..., XComArg]:
    """
    Create an ETL task for extracting, transforming and loading data from a Grist table that:
    1. Gets configuration using selecteur
    2. Reads file from S3
    3. Processes the data (optional)
    4. Writes result to S3 as parquet

    Args:
        selecteur: Configuration selector key
        doc_selecteur: Configuration selector for the Grist document
        normalisation_process_func: Optional normalization process to run
            before the process function
        process_func: Optional function to process the DataFrame

    Returns:
        Callable: Airflow task function
        ```
    """
    if doc_selecteur is None:
        doc_selecteur = "grist_doc"

    @task(task_id=selecteur)
    def _task(**context) -> None:
        """The actual ETL task function."""
        # Get project name from context
        nom_projet = get_project_name(context=context)

        # Get config values related to the task
        task_config = get_source_grist(nom_projet=nom_projet, selecteur=selecteur)
        doc_config = get_source_grist(nom_projet=nom_projet, selecteur=doc_selecteur)
        doc_local_path = Path("/tmp") / doc_config.filename

        if task_config.id_source is None:
            raise ValueError(f"nom_source must be defined for selecteur {selecteur}")

        # Initialize hooks
        s3_handler = create_default_s3_handler()
        local_handler = create_local_handler()
        sqlite_handler = create_db_handler(
            connection_id=str(doc_local_path), db_type=DatabaseType.SQLITE
        )

        # Download Grist doc from S3 to local temp file
        grist_doc = s3_handler.read(file_path=doc_config.filepath_tmp_s3)
        local_handler.write(file_path=str(doc_local_path), content=grist_doc)

        # Get data of table
        df = sqlite_handler.fetch_df(query=f"SELECT * FROM {task_config.id_source}")

        if normalisation_process_func is not None:
            df = normalisation_process_func(df)
        else:
            logging.info(
                msg="No normalisation process function provided. Skipping the normalisation step ..."
            )

        # Removing Grist internal colums
        df = remove_grist_internal_cols(df=df)

        df_info(df=df, df_name=f"{selecteur} - Source normalisée")
        if process_func is not None:
            df = process_func(df)
        else:
            logging.info(
                msg="No process function provided. Skipping the processing step ..."
            )
        df_info(df=df, df_name=f"{selecteur} - After processing")

        # Export
        s3_handler.write(
            file_path=str(task_config.filepath_tmp_s3),
            content=df.to_parquet(path=None, index=False),
        )

        if version == "v2":
            db_schema = get_db_info(context=context).prod_schema
            properties = generate_catalog_properties(
                uri=DEFAULT_POLARIS_HOST,
            )
            catalog = IcebergCatalog(name="data_store", properties=properties)

            key_split = task_config.filepath_s3.split(sep="/")
            tbl_name = key_split.pop(-1)
            namespace = ".".join([db_schema] + key_split)
            write_to_s3(
                catalog=catalog,
                df=df,
                table_status=IcebergTableStatus.STAGING,
                namespace=namespace,
                table_name=tbl_name,
            )

    return _task


def create_file_etl_task(
    selecteur: str,
    process_func: Optional[Callable[..., pd.DataFrame]] = None,
    read_options: Optional[dict[str, Any]] = None,
    apply_cols_mapping: bool = True,
    add_import_date: bool = True,
    add_snapshot_id: bool = True,
) -> Callable[..., XComArg]:
    """Create an ETL task for extracting, transforming and loading data from a file.

    Args:
        selecteur: The identifier for this ETL task
        process_func: Optional function to process the DataFrame
        read_options: Optional options for reading the source file

    Returns:
        An Airflow task that performs the ETL operation
    """

    @task(task_id=selecteur)
    def _task(**context) -> None:
        """The actual ETL task function."""
        # Get project name from context
        nom_projet = get_project_name(context=context)

        # Initialize hooks
        s3_handler = create_default_s3_handler()

        # Get config values related to the task
        task_config = get_source_fichier(nom_projet=nom_projet, selecteur=selecteur)

        # Get data of table
        df = read_dataframe(
            file_handler=s3_handler,
            file_path=task_config.filepath_source_s3,
            read_options=read_options,
        )

        df_info(df=df, df_name=f"{selecteur} - Source normalisée")
        if apply_cols_mapping:
            # Apply column mapping if available
            cols_mapping = column_mapping_dataframe(
                nom_projet=nom_projet, selecteur=selecteur
            )
            if cols_mapping.empty:

                logging.info(f"No column mapping found for selecteur {selecteur}")
            else:
                logging.info(
                    "apply_cols_mapping set to True. Renaming the dataframe labels ..."
                )
                cols_mapping = column_mapping_dict(df_cols_map=cols_mapping)
                logging.info(f"Columns mapping: \n{cols_mapping}")
                df = df.set_axis(
                    labels=[" ".join(colname.split()) for colname in df.columns],
                    axis="columns",
                )
                df = df.rename(columns=cols_mapping)
                df = df.loc[:, list(cols_mapping.values())]

        if process_func is None:
            logging.info(
                "No process function provided. Skipping the processing step ..."
            )
        else:
            df_info(df=df, df_name=f"{selecteur} - After column mapping")
            df = process_func(df)

        if add_import_date:
            df = _add_import_metadata(df=df, context=context)

        if add_snapshot_id:
            df = _add_snapshot_id_metadata(df=df, context=context)

        df_info(df=df, df_name=f"{selecteur} - After processing")

        # Export
        s3_handler.write(
            file_path=str(task_config.filepath_tmp_s3),
            content=df.to_parquet(path=None, index=False),
        )

    return _task


def _execute_step(
    *,
    step: ETLStep,
    previous_output: Any,
    context: dict,
    s3_handler,
    nom_projet: str,
    input_selecteurs: list[str] | None,
) -> Any:
    fn = step.fn
    kwargs = dict(step.kwargs or {})

    if step.read_data and input_selecteurs is None:
        raise ValueError("input_selecteurs must be provided if step.read_data is True")

    # Inject context if required
    if step.use_context:
        kwargs["context"] = context

    # Read data if required
    if step.read_data and input_selecteurs:
        for sel in input_selecteurs:
            cfg = get_selector_info(nom_projet=nom_projet, selecteur=sel)
            df = read_dataframe(
                file_handler=s3_handler,
                file_path=cfg.filepath_tmp_s3,
            )
            df_info(df=df, df_name=f"{sel} - Input dataframe")
            # Use "df" as key if only one input, otherwise "df_{selecteur}"
            key = "df" if len(input_selecteurs) == 1 else f"df_{sel}"
            kwargs[key] = df

    # Execute function
    logging.info(msg=f"Executing function: {fn.__name__} with kwargs: {kwargs.keys()}")
    if step.use_previous_output:
        return fn(previous_output, **kwargs)

    return fn(**kwargs)


def create_task(
    task_config: TaskConfig,
    output_selecteur: str,
    steps: list[ETLStep],
    input_selecteurs: Optional[list[str]] = None,
    add_import_date: bool = True,
    add_snapshot_id: bool = True,
    export_output: bool = True,
) -> Callable[..., XComArg]:
    """
    Create a generic Airflow task based on the provided TaskConfig.

    Args:
        task_config: Configuration for the task
        output_selecteur: Selector for the output configuration
        steps: List of ETL steps to execute
        input_selecteurs: (Optional) list of selectors for input data
        add_import_date: Whether to add import date metadata to the output
        add_snapshot_id: Whether to add snapshot ID metadata to the output
        export_output: Whether to export the final output to S3

    Returns:
        An Airflow task that performs the defined ETL steps

    Note:
        input_selecteurs:
            - must be provided if any step requires reading data
            - if there is a single input selector, the DataFrame will be passed as "df".
            - if multiple input selectors, DataFrames will be passed as "df_{selecteur}"
    """

    @task(
        task_id=task_config.task_id,
        retries=task_config.retries,
        retry_delay=task_config.retry_delay,
        retry_exponential_backoff=task_config.retry_exponential_backoff,
        max_retry_delay=task_config.max_retry_delay,
        on_execute_callback=task_config.on_execute_callback,
        on_failure_callback=task_config.on_failure_callback,
        on_success_callback=task_config.on_success_callback,
        on_retry_callback=task_config.on_retry_callback,
        on_skipped_callback=task_config.on_skipped_callback,
    )
    def _task(**context) -> None:
        """The actual generic task function."""
        # Get project name from context
        nom_projet = get_project_name(context=context)

        # Initialize handler
        s3_handler = create_default_s3_handler()

        # Execute steps
        result: Any = None
        for idx, step in enumerate(steps):
            logging.info(msg=f"▶ Executing step: step_{idx}")
            logging.info(msg=f"Step information: {step}")

            result = _execute_step(
                step=step,
                previous_output=result,
                context=context,
                s3_handler=s3_handler,
                nom_projet=nom_projet,
                input_selecteurs=input_selecteurs,
            )

            if isinstance(result, pd.DataFrame):
                df_info(df=result, df_name=f"step_{idx} - output")

        # Export final result - always a DataFrame and the last step output
        if not export_output:
            return

        if not isinstance(result, pd.DataFrame):
            raise ValueError(
                "Final output must be a pandas DataFrame when export_output=True"
            )

        # Resolve configs
        output_config = get_selector_info(
            nom_projet=nom_projet, selecteur=output_selecteur
        )

        if add_import_date:
            result = _add_import_metadata(df=result, context=context)

        if add_snapshot_id:
            result = _add_snapshot_id_metadata(df=result, context=context)

        df_info(df=result, df_name=f"{output_selecteur} - df to export")

        s3_handler.write(
            file_path=str(output_config.filepath_tmp_s3),
            content=result.to_parquet(path=None, index=False),
        )

    return _task
