from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src._types.dags import ETLStep, TaskConfig
from dags.sg.siep.mmsi.api_operat import actions
from src.common_tasks.etl import create_task

from dags.sg.siep.mmsi.api_operat import process


@task_group
def source() -> None:
    declarations = create_task(
        task_config=TaskConfig(task_id="liste_declaration"),
        output_selecteur="declarations",
        steps=[
            ETLStep(
                fn=actions.liste_declaration,
                use_context=True,
            ),
        ],
        add_import_date=False,
        add_snapshot_id=False,
        export_output=True,
    )
    consommations = create_task(
        task_config=TaskConfig(task_id="consommation_by_id"),
        output_selecteur="consommations",
        steps=[
            ETLStep(
                fn=actions.consommation_by_id,
                use_context=True,
            ),
        ],
        add_import_date=False,
        add_snapshot_id=False,
        export_output=True,
    )

    chain(
        declarations(),
        consommations(),
    )


@task_group
def output() -> None:
    declarations = create_task(
        task_config=TaskConfig(task_id="liste_declaration"),
        input_selecteurs=["declarations"],
        output_selecteur="declarations",
        steps=[
            ETLStep(
                fn=actions.liste_declaration,
                use_context=True,
                read_data=True,
            ),
        ],
        export_output=True,
    )
    adresses_efa = create_task(
        task_config=TaskConfig(task_id="liste_declaration"),
        input_selecteurs=["declarations"],
        output_selecteur="adresses_efa",
        steps=[
            ETLStep(
                fn=actions.liste_declaration,
                use_context=True,
                read_data=True,
            ),
        ],
        export_output=True,
    )
    activite = create_task(
        task_config=TaskConfig(task_id="liste_declaration"),
        input_selecteurs=["consommations"],
        output_selecteur="declarations",
        steps=[
            ETLStep(
                fn=process.process_detail_conso_activite,
                use_context=True,
                read_data=True,
            ),
        ],
        export_output=True,
    )
    indicateur = create_task(
        task_config=TaskConfig(task_id="liste_declaration"),
        input_selecteurs=["consommations"],
        output_selecteur="declarations",
        steps=[
            ETLStep(
                fn=process.process_detail_conso_indicateur,
                use_context=True,
                read_data=True,
            ),
        ],
        export_output=True,
    )
    detail = create_task(
        task_config=TaskConfig(task_id="liste_declaration"),
        input_selecteurs=["consommations"],
        output_selecteur="declarations",
        steps=[
            ETLStep(
                fn=process.process_detail_conso,
                use_context=True,
                read_data=True,
            ),
        ],
        export_output=True,
    )

    chain(
        declarations(),
        adresses_efa(),
        activite(),
        indicateur(),
        detail(),
    )
