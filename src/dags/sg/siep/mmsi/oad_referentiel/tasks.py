from src._types.tasks import DataFrameStep, ETLTask
from src._types.readers import GristReaderStrategy
from src._types.writers import FileWriterStrategy
from src._types.dags import TaskConfig

from src.dags.sg.siep.mmsi.oad_referentiel import process

ref_typologie = ETLTask(
    task_config=TaskConfig(task_id="ref_typologie"),
    target="ref_typologie",
    reader=GristReaderStrategy(),
    steps=[
        DataFrameStep(
            fn=process.process_ref_typologie,
            input_key="ref_typologie",
            output_key="ref_typologie",
        )
    ],
    writers=[FileWriterStrategy()],
    add_metadata=True,
).create_task()
