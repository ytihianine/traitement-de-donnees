from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.utils.tasks.etl import (
    create_file_etl_task,
)

from src.dags.dge.carto_rem.fichiers import process


@task_group
def source_files() -> None:
    agent_info_carriere = create_file_etl_task(
        selecteur="agent_info_carriere",
        process_func=process.process_agent_info_carriere,
    )
    agent_contrat = create_file_etl_task(
        selecteur="agent_contrat",
        process_func=process.process_agent_contrat,
    )
    agent_r4 = create_file_etl_task(
        selecteur="agent_r4",
        process_func=process.process_agent_r4,
    )

    # ordre des tâches
    chain([agent_info_carriere(), agent_contrat(), agent_r4()])
