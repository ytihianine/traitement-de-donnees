from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.snum.certificats_igc import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


def _source_file_pipeline(dataset_name: str, custom_fn) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=custom_fn,
        add_metadata=True,
    )


@task_group(group_id="source_files")
def source_files() -> None:
    agent = create_task(
        pipeline=_source_file_pipeline("agent", process.process_agent),
        execution_options=config.execution_options,
    )
    certificat = create_task(
        pipeline=_source_file_pipeline("certificat", process.process_certificat),
        execution_options=config.execution_options,
    )
    mandataire = create_task(
        pipeline=_source_file_pipeline("mandataire", process.process_mandataire),
        execution_options=config.execution_options,
    )

    # ordre des tâches
    chain([agent(), certificat(), mandataire()])
