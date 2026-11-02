from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.demo import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group
def referentiels() -> None:
    ref_direction = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_direction"),),
            output_dataset=Dataset(name="ref_direction"),
            operation=process.process_ref_direction,
        ),
        execution_options=config.execution_options,
    )
    ref_type_demande = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="ref_type_demande"),),
            output_dataset=Dataset(name="ref_type_demande"),
            operation=process.process_ref_type_demande,
        ),
        execution_options=config.execution_options,
    )

    # Ordre des tâches
    chain(
        [
            ref_direction(),
            ref_type_demande(),
        ]
    )


@task_group
def activite() -> None:
    demande = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="demande"),),
            output_dataset=Dataset(name="demande"),
            operation=process.process_demande,
        ),
        execution_options=config.execution_options,
    )
    equipe = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="equipe"),),
            output_dataset=Dataset(name="equipe"),
            operation=process.process_equipe,
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain([demande(), equipe()])
