from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.fichiers import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group(group_id="source_files")
def source_files() -> None:
    agent_info_carriere = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="agent_info_carriere"),),
            output_dataset=Dataset(name="agent_info_carriere"),
            operation=process.process_agent_info_carriere,
        ),
        execution_options=config.execution_options,
    )
    agent_remuneration = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="agent_remuneration"),),
            output_dataset=Dataset(name="agent_remuneration"),
            operation=process.process_agent_remuneration,
        ),
        execution_options=config.execution_options,
    )

    chain([agent_info_carriere(), agent_remuneration()])
