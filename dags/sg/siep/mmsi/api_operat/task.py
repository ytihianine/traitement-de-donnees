from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import ETLStep, TaskConfig
from utils.tasks.etl import create_task

from dags.sg.siep.mmsi.api_operat import actions


@task_group
def taches():
    declarations = create_task(
        task_config=TaskConfig(task_id="liste_declaration"),
        output_selecteur="declarations",
        steps=[
            ETLStep(
                fn=actions.liste_declaration,
                use_context=True,
            ),
        ],
        export_output=False,
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
        export_output=False,
    )

    chain(
        declarations(),
        consommations(),
    )
