from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import ETLStep, TaskConfig
from utils.tasks.etl import (
    create_file_etl_task,
    create_task,
)

from dags.dge.carto_rem.fichiers import process


@task_group
def source_files() -> None:
    # agent_elem_rem = create_file_etl_task(
    #     selecteur="agent_elem_rem",
    #     process_func=process.process_agent_elem_rem,
    #     add_import_date=False,
    #     add_snapshot_id=False,
    # )
    agent_info_carriere = create_file_etl_task(
        selecteur="agent_info_carriere",
        process_func=process.process_agent_info_carriere,
        add_import_date=False,
        add_snapshot_id=False,
    )
    agent_contrat = create_file_etl_task(
        selecteur="agent_contrat",
        process_func=process.process_agent_contrat,
        add_import_date=False,
        add_snapshot_id=False,
    )

    # ordre des tâches
    chain([agent_info_carriere(), agent_contrat()])


@task_group
def output_files() -> None:
    agent = create_task(
        task_config=TaskConfig(task_id="agent"),
        output_selecteur="agent",
        input_selecteurs=[
            "agent_info_carriere",
        ],
        steps=[
            ETLStep(
                fn=process.process_agent,
                read_data=True,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    agent_carriere = create_task(
        task_config=TaskConfig(task_id="agent_carriere"),
        output_selecteur="agent_carriere",
        input_selecteurs=[
            "agent_info_carriere",
        ],
        steps=[
            ETLStep(
                fn=process.process_agent_carriere,
                read_data=True,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )

    # ordre des tâches
    chain([agent(), agent_carriere()])
