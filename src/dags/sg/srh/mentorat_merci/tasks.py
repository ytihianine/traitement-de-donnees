from src._types.tasks import SingleInputStep, ETLTask
from src._types.readers import GristReaderStrategy
from src._types.writers import FileWriterStrategy
from src._types.dags import TaskConfig

from src._types.dags import ETLStep
from src.common_tasks.etl import create_task

from src.dags.sg.srh.mentorat_merci import action
from src.dags.sg.srh.mentorat_merci import process

agent_inscrit = ETLTask(
    task_config=TaskConfig(task_id="agent_inscrit"),
    target="agent_inscrit",
    reader=GristReaderStrategy(),
    steps=[
        SingleInputStep(
            fn=process.clean_data,
            input_key="agent_inscrit",
            output_key="agent_inscrit",
        )
    ],
    writers=[FileWriterStrategy()],
    add_metadata=True,
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
