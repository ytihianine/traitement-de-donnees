from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.common_tasks.etl import create_file_etl_task

from src.dags.sg.snum.certificats_igc import process


@task_group
def source_files() -> None:
    agent = create_file_etl_task(selecteur="agent", process_func=process.process_agent)
    certificat = create_file_etl_task(
        selecteur="certificat",
        process_func=process.process_certificat,
        read_options={"sep": ";"},
    )
    mandataire = create_file_etl_task(
        selecteur="mandataire", process_func=process.process_mandataire
    )

    # ordre des tâches
    chain([agent(), certificat(), mandataire()])
