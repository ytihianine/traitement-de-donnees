from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.snum.certificats_igc import process
from modules.types.dags import TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy


@task_group
def source_files() -> None:
    agent = ETLTask(
        task_config=TaskConfig(task_id="agent"),
        target="agent",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_agent,
                input_key="agent",
                output_key="agent",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    certificat = ETLTask(
        task_config=TaskConfig(task_id="certificat"),
        target="certificat",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_certificat,
                input_key="certificat",
                output_key="certificat",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    mandataire = ETLTask(
        task_config=TaskConfig(task_id="mandataire"),
        target="mandataire",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=process.process_mandataire,
                input_key="mandataire",
                output_key="mandataire",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # ordre des tâches
    chain([agent.create_task(), certificat.create_task(), mandataire.create_task()])
