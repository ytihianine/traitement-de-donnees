from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import ETLStep, TaskConfig
from utils.tasks.etl import create_file_etl_task, create_task

from dags.sg.snum.certificats_igc import process


@task_group
def source_files() -> None:
    agents = create_file_etl_task(
        selecteur="agents", process_func=process.process_agents
    )
    aip = create_file_etl_task(selecteur="aip", process_func=process.process_aip)
    certificat = create_file_etl_task(
        selecteur="certificat",
        process_func=process.process_certificat,
        read_options={"sep": ";"},
    )
    historique_certificat = create_file_etl_task(
        selecteur="historique_certificat",
        process_func=process.process_historique_certificat,
    )
    mandataire = create_file_etl_task(
        selecteur="mandataire", process_func=process.process_mandataire
    )

    # ordre des tâches
    chain([agents(), aip(), certificat(), historique_certificat(), mandataire()])


@task_group
def output_files() -> None:
    liste_certificats = create_task(
        task_config=TaskConfig(task_id="liste_certificats"),
        output_selecteur="liste_certificats",
        input_selecteurs=["certificats", "agents"],
        steps=[ETLStep(fn=process.process_liste_certificats, read_data=True)],
    )

    # ordre des tâches
    chain([liste_certificats()])
