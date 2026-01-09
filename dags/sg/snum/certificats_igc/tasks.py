from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from entities.dags import ETLStep, TaskConfig
from utils.tasks.etl import create_file_etl_task, create_task

from dags.sg.snum.certificats_igc import process


@task_group
def source_files() -> None:
    agents = create_file_etl_task(
        selecteur="agents", process_func=process.process_agents
    )
    aip = create_file_etl_task(
        selecteur="aip", process_func=process.process_aip, read_options={"sep": ";"}
    )
    certificats = create_file_etl_task(
        selecteur="certificats",
        process_func=process.process_certificats,
        read_options={"sep": ";"},
    )
    igc = create_file_etl_task(selecteur="igc", process_func=process.process_igc)

    # ordre des tâches
    chain([agents(), aip(), certificats(), igc()])


@task_group
def output_files() -> None:
    liste_aip = create_task(
        task_config=TaskConfig(task_id="liste_aip"),
        output_selecteur="liste_aip",
        input_selecteurs=["igc", "agents"],
        steps=[
            ETLStep(
                fn=process.process_liste_aip,
            )
        ],
    )
    liste_certificats = create_task(
        task_config=TaskConfig(task_id="liste_certificats"),
        output_selecteur="liste_certificats",
        input_selecteurs=["certificats", "agents"],
        steps=[
            ETLStep(
                fn=process.process_liste_certificats,
            )
        ],
    )

    # ordre des tâches
    chain([liste_aip(), liste_certificats()])
