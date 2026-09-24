import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

import pandas as pd
from airflow.sdk import XComArg, task
from airflow.sdk.definitions._internal.abstractoperator import TaskStateChangeCallback

from modules.domain.pipeline.model import ExecutionOptions, PipelineDescriptor
from modules.domain.projet.model import ProjetMetadata
from modules.infra.airflow.dag import AirflowDagRepository
from modules.infra.database.postgres.projet_repository import PostgresProjetRepository
from modules.infra.file_system.data_readers import FileDatasetReader
from modules.infra.file_system.data_writers import FileDatasetWriter
from modules.infra.file_system.factory import FileHandlerType, FSConfig
from modules.logs import df_info


@dataclass(frozen=True)
class TaskConfig:
    task_id: str
    retries: int = 0
    retry_delay: timedelta | float = 0
    retry_exponential_backoff: bool = False
    max_retry_delay: timedelta | float | None = None
    on_execute_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_failure_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_success_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_retry_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_skipped_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None


def _add_metadata(df: pd.DataFrame, metadata: ProjetMetadata) -> pd.DataFrame:

    df["import_timestamp"] = metadata.import_timestamp
    df["snapshot_id"] = str(metadata.snapshot_id)
    df["snapshot_id_parent"] = str(metadata.snapshot_id_parent) if metadata.snapshot_id_parent is not None else None

    return df


def create_task(
    config: TaskConfig,
    pipeline: PipelineDescriptor,
    execution_options: ExecutionOptions,
) -> Callable[..., XComArg]:
    """
    Create a generic Airflow task based on the provided TaskConfig.

    Args:
        config: Configuration for the task
        pipeline: Pipeline descriptor
        execution_options: Execution options for the task

    Returns:
        An Airflow task that performs the defined ETL steps

    Note:
        input_selecteurs:
            - must be provided if any step requires reading data
            - if there is a single input selector, the DataFrame will be passed as "df".
            - if multiple input selectors, DataFrames will be passed as "df_{selecteur}"
    """

    @task(
        task_id=config.task_id,
        retries=config.retries,
        retry_delay=config.retry_delay,
        retry_exponential_backoff=config.retry_exponential_backoff,
        max_retry_delay=config.max_retry_delay,
        on_execute_callback=config.on_execute_callback,
        on_failure_callback=config.on_failure_callback,
        on_success_callback=config.on_success_callback,
        on_retry_callback=config.on_retry_callback,
        on_skipped_callback=config.on_skipped_callback,
    )
    def _task(**context) -> None:
        """The actual generic task function."""
        # Hooks & variables
        dag_repository = AirflowDagRepository()
        nom_projet = dag_repository.get_project_name(context=context)

        # Read data
        input_data = {}
        for dataset in pipeline.input_datasets:
            logging.info(msg=f"▶ Reading dataset: {dataset.name}")
            reader = FileDatasetReader(
                fs_config=FSConfig(
                    bucket=dataset.storage.bucket,
                    connection_id=dataset.storage.s3_conn_id,
                ),
                fs_type=FileHandlerType.S3,
            )
            df = reader.read(dataset=dataset)
            input_data[dataset.name] = df

        # Apply transformations
        result = pd.DataFrame()  # Initialize an empty DataFrame to hold the result
        for idx, step in enumerate(pipeline.transformations):
            logging.info(msg=f"▶ Executing transformation: step_{idx}")
            logging.info(msg=f"Transformation information: {step}")
            result = step(result)

        if execution_options.add_metadata:
            projet_repository = PostgresProjetRepository()
            projet_metadata = projet_repository.get_projet_metadata(nom_projet=nom_projet)
            result = _add_metadata(df=result, metadata=projet_metadata)

        # Log the final DataFrame information
        df_info(df=result, df_name=f"{pipeline.output_dataset.name} - df to export")

        # Export final result - always a DataFrame and the last step output
        if not execution_options.export_result:
            return

        writer = FileDatasetWriter(
            fs_config=FSConfig(
                bucket=pipeline.output_dataset.storage.bucket,
                connection_id=pipeline.output_dataset.storage.s3_conn_id,
            ),
            fs_type=FileHandlerType.S3,
        )
        writer.write(df=result, dataset=pipeline.output_dataset)

    return _task
