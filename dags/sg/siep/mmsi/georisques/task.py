from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import (
    create_action_from_multi_input_files_etl_task,
    create_action_to_file_etl_task,
)

from dags.sg.siep.mmsi.georisques import actions


@task_group
def georisques_group() -> None:
    """Task group for the Georisques pipeline."""

    bien_db = create_action_to_file_etl_task(
        output_selecteur="bien_db",
        task_id="bien_db",
        action_func=actions.get_bien_from_db,
    )

    georisques = create_action_from_multi_input_files_etl_task(
        task_id="georisques",
        input_selecteurs=["bien_db"],
        action_func=actions.get_georisques,
    )

    chain(
        bien_db(),
        georisques(),
    )
