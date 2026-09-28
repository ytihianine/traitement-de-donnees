from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.fichiers import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group(group_id="source_files")
def source_files() -> None:
    agent_carriere = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("agent_carriere"),),
            output_dataset=Dataset("agent_carriere"),
            operation=process.process_agent_info_carriere,
        ),
        execution_options=config.execution_options,
    )
    agent = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("agent"),),
            output_dataset=Dataset("agent"),
            operation=process.process_agent_contrat,
        ),
        execution_options=config.execution_options,
    )
    agent_elem_rem = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("agent_elem_rem"),),
            output_dataset=Dataset("agent_elem_rem"),
            operation=process.process_agent_r4,
        ),
        execution_options=config.execution_options,
    )

    chain([agent_carriere(), agent(), agent_elem_rem()])
