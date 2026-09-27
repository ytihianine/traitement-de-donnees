import logging
from collections.abc import Callable

import pandas as pd
from airflow.sdk import XComArg, task

from modules.containers import DEFAULT_DAG_REPO, DEFAULT_DATASET_CONTEXT_REPO, DEFAULT_PROJET_REPO
from modules.domain.dag.repository import DagRepository
from modules.domain.dataset.repository import DatasetContextRepository
from modules.domain.pipeline.model import ExecutionOptions, PipelineDescriptor
from modules.domain.pipeline.output import DEFAULT_OUTPUT_ADAPTER_REGISTRY, OutputAdapterRegistry
from modules.domain.projet.model import ProjetMetadata
from modules.domain.projet.repository import ProjetRepository
from modules.infra.file_system.dataset_location_factory import create_dataset_location_provider
from modules.logs import df_info


def _add_metadata(df: pd.DataFrame, metadata: ProjetMetadata) -> pd.DataFrame:

    df["import_timestamp"] = metadata.import_timestamp
    df["snapshot_id"] = str(metadata.snapshot_id)
    df["snapshot_id_parent"] = str(metadata.snapshot_id_parent) if metadata.snapshot_id_parent is not None else None

    return df


def create_task(
    pipeline: PipelineDescriptor,
    execution_options: ExecutionOptions,
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    projet_repo: ProjetRepository = DEFAULT_PROJET_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    output_adapter_registry: OutputAdapterRegistry = DEFAULT_OUTPUT_ADAPTER_REGISTRY,
) -> Callable[..., XComArg]:
    """
    Create a generic Airflow task based on the provided TaskConfig.

    Args:
        config: Configuration for the task
        pipeline: Pipeline descriptor
        execution_options: Execution options for the task
        dag_repo: Repository for interacting with Airflow DAGs, defaults to DEFAULT_DAG_REPO
        projet_repo: Repository for interacting with project storage, defaults to DEFAULT_PROJET_REPO
        dataset_context_repo: Repository for interacting with project storage, defaults to DEFAULT_DATASET_CONTEXT_REPO
        output_adapter_registry: Registry used to serialize pipeline operation results before storage

    Returns:
        An Airflow task that performs the defined ETL steps

    Note:
        input_selecteurs:
            - must be provided if any step requires reading data
            - if there is a single input selector, the DataFrame will be passed as "df".
            - if multiple input selectors, DataFrames will be passed as "df_{selecteur}"
    """

    @task(
        task_id=pipeline.output_dataset.name,
    )
    def _task(**context) -> None:
        """The actual generic task function."""
        # Hooks & variables
        nom_projet = dag_repo.get_project_name(context=context)

        # Read data
        input_data = {}
        for dataset in pipeline.input_datasets:
            logging.info(msg=f"▶ Reading dataset: {dataset.name}")
            dataset_context = dataset_context_repo.get(nom_projet=nom_projet, nom_dataset=dataset.name)
            if pipeline.use_input_results_as_operation_args:
                dataset_location = dataset_context.tmp_location
            else:
                dataset_location = dataset_context.src_location

            reader = create_dataset_location_provider(dataset_location=dataset_location)
            df = reader.read(location=dataset_location.validate_location)
            input_data[f"df_{dataset.name}"] = df

        if len(input_data) == 1:
            input_data = {"df": next(iter(input_data.values()))}

        # Apply operations
        logging.info(msg=f"Running pipeline operation: {pipeline.operation.__name__}")
        result = pipeline.operation(**input_data)

        if result is None:
            logging.warning(msg="Pipeline operation returned None. Ending pipeline execution.")
            return

        if pipeline.add_metadata:
            if not isinstance(result, pd.DataFrame):
                raise TypeError("add_metadata is only supported for DataFrame results")
            projet_metadata = projet_repo.get_projet_metadata(nom_projet=nom_projet)
            result = _add_metadata(df=result, metadata=projet_metadata)

        if isinstance(result, pd.DataFrame):
            df_info(df=result, df_name=f"{pipeline.output_dataset.name} - df to export")

        output_dataset_context = dataset_context_repo.get(
            nom_projet=nom_projet, nom_dataset=pipeline.output_dataset.name
        )
        output_location = output_dataset_context.dest_location
        provider = create_dataset_location_provider(dataset_location=output_location)
        adapter = output_adapter_registry.get_adapter(result)
        logging.info(
            msg=(
                f"Exporting pipeline result of type {type(result).__name__} "
                f"using {type(adapter).__name__} to {output_location.validate_location}"
            )
        )
        adapter.write(
            output=result,
            provider=provider,
            location=output_location.validate_location,
        )

    return _task
