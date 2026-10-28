from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.finances.suivi_budgetaire import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group()
def sources() -> None:
    nomenclature = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="nomenclature"),),
            output_dataset=Dataset(name="nomenclature"),
            operation=process.process_nomenclature,
        ),
        execution_options=config.execution_options,
    )
    programmation_annuelle = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="programmation_annuelle"),),
            output_dataset=Dataset(name="programmation_annuelle"),
            operation=process.process_programmation_annuelle,
        ),
        execution_options=config.execution_options,
    )
    execution_mensuelle = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="execution_mensuelle"),),
            output_dataset=Dataset(name="execution_mensuelle"),
            operation=process.process_execution_mensuelle,
        ),
        execution_options=config.execution_options,
    )
    lfi = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="lfi"),),
            output_dataset=Dataset(name="lfi"),
            operation=process.process_lfi,
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain([nomenclature(), programmation_annuelle(), execution_mensuelle(), lfi()])
