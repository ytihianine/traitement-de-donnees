from entities.dags import ALL_PARAM_PATHS, ETLStep, TaskConfig
from utils.tasks.etl import create_task
from utils.tasks.validation import create_validate_params_task

from dags.sg.srh.mentorat_merci import action


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


generer_binomes = create_task(
    task_config=TaskConfig(task_id="generer_binomes"),
    output_selecteur="agent_inscrit",
    input_selecteurs=["agent_inscrit"],
    steps=[
        ETLStep(fn=action.trouver_meilleurs_binomes, read_data=True),
        ETLStep(fn=action.send_result, use_context=True, use_previous_output=True),
    ],
    add_import_date=False,
    add_snapshot_id=False,
    export_output=False,
)
