from dags.sg.srh.mentorat_merci import actions, config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task

agent_inscrit = create_task(
    pipeline=PipelineDescriptor(
        input_datasets=(Dataset("agent_inscrit"),),
        output_dataset=Dataset("agent_inscrit"),
        operation=process.clean_data,
    ),
    execution_options=config.execution_options,
)

generer_binomes = create_task(
    pipeline=PipelineDescriptor(
        input_datasets=(Dataset("agent_inscrit"),),
        output_dataset=Dataset("generer_binomes"),
        operation=actions.action_generer_binomes_mentorat,
    ),
    execution_options=config.execution_options,
)
