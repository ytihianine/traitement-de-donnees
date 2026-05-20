from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src._types.dags import TaskConfig, ETLStep

from src.common_tasks.etl import create_file_etl_task, create_task

from src.dags.sg.snum.certificats_igc import process


@task_group
def source_files() -> None:
    agent = create_file_etl_task(selecteur="agent", process_func=process.process_agent)
    aip = create_file_etl_task(selecteur="aip", process_func=process.process_aip)
    certificat = create_file_etl_task(
        selecteur="certificat",
        process_func=process.process_certificat,
        read_options={"sep": ";"},
    )
    historique_certificat = create_file_etl_task(
        selecteur="historique_certificat",
        process_func=process.process_historique_certificat,
        read_options={"sep": ";"},
    )
    mandataire = create_file_etl_task(
        selecteur="mandataire", process_func=process.process_mandataire
    )

    # ordre des tâches
    chain([agent(), aip(), certificat(), historique_certificat(), mandataire()])


@task_group
def additionnal_tasks() -> None:
    certificat_contact = create_task(
        task_config=TaskConfig(task_id="certificat_contact"),
        output_selecteur="certificat_contact",
        input_selecteurs=["certificat", "agent"],
        steps=[ETLStep(fn=process.process_certificat_contact, read_data=True)],
    )

    # ordre des tâches
    chain(certificat_contact())
