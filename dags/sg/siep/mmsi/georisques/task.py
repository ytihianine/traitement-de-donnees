from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.config.types import ETLStep, TaskConfig
from utils.tasks.etl import (
    create_task,
)

from dags.sg.siep.mmsi.georisques import actions


@task_group
def georisques_group() -> None:
    """Task group for the Georisques pipeline."""

    bien_db = create_task(
        task_config=TaskConfig(task_id="bien_db"),
        output_selecteur="bien_db",
        steps=[
            ETLStep(
                fn=actions.get_bien_from_db,
                use_context=True,
            )
        ],
        add_snapshot_id=False,
        add_import_date=False,
    )

    georisques = create_task(
        task_config=TaskConfig(task_id="georisques"),
        output_selecteur="georisques",
        input_selecteurs=["bien_db"],
        steps=[
            ETLStep(
                fn=actions.get_georisques,
            )
        ],
    )

    chain(
        bien_db(),
        georisques(),
    )
