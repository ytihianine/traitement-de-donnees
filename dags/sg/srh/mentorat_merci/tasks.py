from entities.dags import ETLStep, TaskConfig
from utils.tasks.etl import create_task

from dags.sg.srh.mentorat_merci import action

generer_binomes = create_task(
    task_config=TaskConfig(task_id="generer_binomes"),
    output_selecteur="liste_certificats",
    input_selecteurs=["certificats", "agents"],
    steps=[
        ETLStep(fn=action.trouver_meilleurs_binomes, read_data=True),
        ETLStep(fn=action.send_result, use_context=True, use_previous_output=True),
    ],
    add_import_date=False,
    add_snapshot_id=False,
    export_output=False,
)
