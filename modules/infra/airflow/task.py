from collections.abc import Callable

from airflow.sdk import XComArg, task

from modules.application.pipeline import PipelineRunner
from modules.containers import (
    DEFAULT_DAG_REPO,
    DEFAULT_DATASET_CONTEXT_REPO,
    DEFAULT_LOCATION_PROVIDER_FACTORY,
    DEFAULT_PROJET_REPO,
)
from modules.domain.dag.repository import DagRepository
from modules.domain.dataset.ports import DatasetLocationProviderFactory
from modules.domain.dataset.repository import DatasetContextRepository
from modules.domain.pipeline.model import ExecutionOptions, PipelineDescriptor
from modules.domain.pipeline.output import DEFAULT_OUTPUT_ADAPTER_REGISTRY, OutputAdapterRegistry
from modules.domain.projet.repository import ProjetRepository


def create_task(
    pipeline: PipelineDescriptor,
    execution_options: ExecutionOptions,
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    projet_repo: ProjetRepository = DEFAULT_PROJET_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    output_adapter_registry: OutputAdapterRegistry = DEFAULT_OUTPUT_ADAPTER_REGISTRY,
    location_provider_factory: DatasetLocationProviderFactory = DEFAULT_LOCATION_PROVIDER_FACTORY,
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

        pipeline_runner = PipelineRunner(
            projet_repo=projet_repo,
            dataset_context_repo=dataset_context_repo,
            output_adapter_registry=output_adapter_registry,
            location_provider_factory=location_provider_factory,
        )
        pipeline_runner.run(nom_projet=nom_projet, pipeline=pipeline, execution_options=execution_options)

    return _task
