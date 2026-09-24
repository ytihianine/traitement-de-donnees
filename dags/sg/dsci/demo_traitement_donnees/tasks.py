from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.demo_traitement_donnees import process
from modules.common_tasks.grist import generic_grist_processing
from modules.types.dags import TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy

# Creation des taches


@task_group
def referentiels() -> None:

    ref_direction = ETLTask(
        task_config=TaskConfig(task_id="ref_direction"),
        target="ref_direction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["direction"],
                    custom_fn=process.process_ref_direction,
                ),
                input_key="ref_direction",
                output_key="ref_direction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_intervention = ETLTask(
        task_config=TaskConfig(task_id="ref_intervention"),
        target="ref_intervention",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["typologie_d_intervention2"],
                    custom_fn=process.process_ref_intervention,
                ),
                input_key="ref_intervention",
                output_key="ref_intervention",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre des tâches
    chain(
        [
            ref_direction.create_task(),
            ref_intervention.create_task(),
        ]
    )


@task_group()
def activite() -> None:
    accompagnement = ETLTask(
        task_config=TaskConfig(task_id="accompagnement"),
        target="accompagnement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "date_de_la_demande",
                        "direction",
                        "accompagnement",
                        "statut",
                        "type_d_intervention",
                        "assigne_a",
                        "niveau_de_complexite",
                        "charge_estimee",
                        "charge_consommee",
                        "ecart_de_charge",
                    ],
                    cols_mapping={
                        "direction": "id_direction",
                        "type_d_intervention": "id_type_intervention",
                        "assigne_a": "id_assignation",
                    },
                    txt_columns=[
                        "accompagnement",
                    ],
                    num_columns=["charge_estimee", "charge_consommee", "ecart_de_charge"],
                    ref_columns=["id_direction", "id_type_intervention", "id_assignation"],
                    custom_fn=process.process_accompagnement,
                ),
                input_key="accompagnement",
                output_key="accompagnement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Ordre des tâches
    chain([accompagnement.create_task()])
