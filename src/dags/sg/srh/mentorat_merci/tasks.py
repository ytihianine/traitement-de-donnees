from src._types.dags import ETLStep, TaskConfig
from src._types.readers import GristReaderStrategy
from src._types.tasks import ETLTask, SingleInputStep
from src._types.writers import FileWriterStrategy
from src.common_tasks.etl import create_task
from src.dags.sg.srh.mentorat_merci import actions, process

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
        ETLStep(fn=actions.trouver_meilleurs_binomes, read_data=True),
        ETLStep(fn=actions.send_result, use_context=True, use_previous_output=True),
    ],
    add_import_date=False,
    add_snapshot_id=False,
    export_output=False,
)
