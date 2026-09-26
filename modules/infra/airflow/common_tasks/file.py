"""File processing task utilities using infrastructure handlers."""

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pandas as pd
from airflow.sdk import task

from modules.constants import DEFAULT_DAG_REPO, DEFAULT_DATASET_CONTEXT_REPO
from modules.domain.dag.model import FeatureFlags
from modules.domain.dag.repository import DagRepository
from modules.domain.dataset.repository import DatasetContextRepository
from modules.infra.airflow.dag import should_skip_task
from modules.infra.airflow.task import TaskConfig
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler
from modules.logs import df_info


def convert_cols_mapping_to_dict(cols_mapping: Sequence[Mapping[str, str]]) -> dict:
    logging.debug("Colonnes du dataframe de mapping: %s", cols_mapping)
    records = {m["colname_source"]: m["colname_dest"] for m in cols_mapping}
    return records


def create_parquet_converter_task(
    dataset_name: str,
    task_config: TaskConfig,
    process_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    read_options: dict[str, Any] | None = None,
    apply_cols_mapping: bool = True,
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    datasetcontext_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
) -> Callable:
    """Create a task that converts files to Parquet format.

    Args:
        config: Airflow task configuration
        dataset_name: Dataset to process
        process_func: Optional function to process DataFrame
        read_options: Optional read options for the input file

    Returns:
        Task function that performs the conversion

    Raises:
        ValueError: If task_id not provided in task_params
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
    def convert_to_parquet(**context) -> None:
        """Convert file to Parquet format and upload to S3."""

        if should_skip_task(context=context, feature_flag=FeatureFlags.CONVERT_FILES):
            return

        # Init vars
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(),
        )

        nom_projet = dag_repo.get_project_name(context=context)

        logging.info(msg=f"Getting configuration for project {nom_projet} and dataset {dataset_name}")
        dataset_context = datasetcontext_repo.get(nom_projet=nom_projet, nom_dataset=dataset_name)
        source_key = dataset_context.storage_info.get_full_s3_key(use_id_source=True)
        dest_tmp_key = dataset_context.storage_info.get_full_s3_key(with_tmp_segment=True, use_id_source=False)

        # Read input file based on extension
        logging.info(msg=f"Reading file from {source_key}")
        df = read_dataframe(
            file_handler=s3_handler,
            file_path=source_key,
            read_options=read_options,
        )

        df_info(df, df_name=f"{task_config.task_id} - Initial state")

        df = df.set_axis(
            labels=[" ".join(colname.split()) for colname in df.columns],
            axis="columns",
        )
        if apply_cols_mapping:
            # Apply column mapping if available
            cols_mapping = datasetcontext_repo.get_list_column_mapping(nom_projet=nom_projet, dataset_name=dataset_name)
            if len(cols_mapping) == 0:
                print(f"No column mapping found for dataset {dataset_name}")
            else:
                cols_mapping = convert_cols_mapping_to_dict(cols_mapping=cols_mapping)
                df = df.rename(columns=cols_mapping, errors="raise")
                df = df.drop(columns=list(set(df.columns) - set(cols_mapping.values())))

        # Apply custom processing
        if process_func:
            df = process_func(df)
            df_info(df, df_name=f"{task_config.task_id} - After processing")

        # Convert to parquet and save
        parquet_data = df.to_parquet(path=None, index=False)
        logging.info(msg=f"Saving to {dest_tmp_key}")
        s3_handler.write(file_path=dest_tmp_key, content=parquet_data)
        logging.info(msg=f"Successfully saved parquet file to {dest_tmp_key}")

    return convert_to_parquet
