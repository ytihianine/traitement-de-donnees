from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.fichiers import process
from modules.common_tasks.etl import create_task
from modules.types.dags import ETLStep, TaskConfig


@task_group
def source_files() -> None:
    agent_info_carriere = create_task(
        task_config=TaskConfig(task_id="agent_info_carriere"),
        output_selecteur="agent_info_carriere",
        input_selecteurs=["agent_info_carriere"],
        steps=[ETLStep(fn=process.process_agent_info_carriere, read_data=True)],
    )
    agent_contrat = create_task(
        task_config=TaskConfig(task_id="agent_contrat"),
        output_selecteur="agent_contrat",
        input_selecteurs=["agent_contrat"],
        steps=[ETLStep(fn=process.process_agent_contrat, read_data=True)],
    )
    agent_r4 = create_task(
        task_config=TaskConfig(task_id="agent_r4"),
        output_selecteur="agent_r4",
        input_selecteurs=["agent_r4"],
        steps=[ETLStep(fn=process.process_agent_r4, read_data=True)],
    )

    # ordre des tâches
    chain([agent_info_carriere(), agent_contrat(), agent_r4()])
