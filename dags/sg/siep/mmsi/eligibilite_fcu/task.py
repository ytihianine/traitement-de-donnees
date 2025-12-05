from utils.tasks.etl import (
    create_action_to_file_etl_task,
    create_multi_files_input_etl_task,
)

from dags.sg.siep.mmsi.eligibilite_fcu.actions import eligibilite_fcu
from dags.sg.siep.mmsi.eligibilite_fcu import process


get_eligibilite_fcu = create_action_to_file_etl_task(
    output_selecteur="fcu",
    task_id="eligibilite_fcu_to_file",
    action_func=eligibilite_fcu,
    use_context=True,
)

process_fcu_result = create_multi_files_input_etl_task(
    input_selecteurs=["fcu"],
    output_selecteur="fcu_result",
    process_func=process.process_result,
)
