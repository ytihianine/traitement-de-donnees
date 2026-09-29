from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.georisques import actions, config
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import (
    create_task,
)


@task_group
def georisques_group() -> None:
    """Task group for the Georisques pipeline."""

    bien_db = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("bien_db"),),
            output_dataset=Dataset("bien_db"),
            operation=actions.get_bien_from_db,
            add_metadata=False,
        ),
        execution_options=config.execution_options,
    )

    georisques = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("bien_db"),),
            output_dataset=Dataset("georisques"),
            operation=actions.get_georisques,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )

    chain(
        bien_db(),
        georisques(),
    )
