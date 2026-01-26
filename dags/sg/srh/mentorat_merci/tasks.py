from _types.dags import ETLStep, TaskConfig
from utils.tasks.etl import create_task, create_file_etl_task

from dags.sg.srh.mentorat_merci import action
from dags.sg.srh.mentorat_merci import process


agent_inscrit = create_file_etl_task(
    selecteur="agent_inscrit",
    process_func=process.clean_data,
    add_import_date=False,
    add_snapshot_id=False,
)


generer_binomes = create_task(
    task_config=TaskConfig(task_id="generer_binomes"),
    output_selecteur="generer_binome",
    input_selecteurs=["agent_inscrit"],
    steps=[
        ETLStep(fn=action.trouver_meilleurs_binomes, read_data=True),
        ETLStep(fn=action.send_result, use_context=True, use_previous_output=True),
    ],
    add_import_date=False,
    add_snapshot_id=False,
    export_output=False,
)
