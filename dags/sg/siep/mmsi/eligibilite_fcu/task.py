from utils.config.types import ETLStep, TaskConfig
from utils.tasks.etl import (
    create_task,
    create_multi_files_input_etl_task,
)

from dags.sg.siep.mmsi.eligibilite_fcu import actions
from dags.sg.siep.mmsi.eligibilite_fcu import process


get_eligibilite_fcu = create_task(
    task_config=TaskConfig(task_id="eligibilite_fcu_to_file"),
    output_selecteur="fcu",
    steps=[
        ETLStep(
            fn=actions.eligibilite_fcu,
            use_context=True,
        )
    ],
    add_snapshot_id=False,
    add_import_date=False,
)

process_fcu_result = create_multi_files_input_etl_task(
    input_selecteurs=["fcu"],
    output_selecteur="fcu_result",
    process_func=process.process_result,
)
