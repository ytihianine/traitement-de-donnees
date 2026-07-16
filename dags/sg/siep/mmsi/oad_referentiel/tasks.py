from dags.sg.siep.mmsi.oad_referentiel import process
from modules.types.dags import TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy

ref_typologie = ETLTask(
    task_config=TaskConfig(task_id="ref_typologie"),
    target="ref_typologie",
    reader=GristReaderStrategy(),
    steps=[
        SingleInputStep(
            fn=process.process_ref_typologie,
            input_key="ref_typologie",
            output_key="ref_typologie",
        )
    ],
    writers=[FileWriterStrategy()],
    add_metadata=True,
).create_task()
