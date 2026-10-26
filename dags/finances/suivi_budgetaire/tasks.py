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
    programmation_budgetaire = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="programmation_budgetaire"),),
            output_dataset=Dataset(name="programmation_budgetaire"),
            operation=process.process_programmation_budgetaire,
        ),
        execution_options=config.execution_options,
    )
    consommation_budgetaire = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="consommation_budgetaire"),),
            output_dataset=Dataset(name="consommation_budgetaire"),
            operation=process.process_consommation_budgetaire,
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
    chain([nomenclature(), programmation_budgetaire(), consommation_budgetaire(), lfi()])
