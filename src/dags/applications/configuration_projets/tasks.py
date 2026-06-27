from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.utils.process.structures import normalize_grist_dataframe
from src._types.tasks import DataFrameStep, ETLTask
from src._types.readers import GristReaderStrategy
from src._types.writers import FileWriterStrategy
from src._types.dags import TaskConfig

from src.dags.applications.configuration_projets import process


@task_group
def process_data() -> None:
    ref_direction = ETLTask(
        task_config=TaskConfig(task_id="direction"),
        target="direction",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="direction",
                output_key="direction",
            ),
            DataFrameStep(
                fn=process.process_direction,
                input_key="direction",
                output_key="direction",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_service = ETLTask(
        task_config=TaskConfig(task_id="service"),
        target="service",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe, input_key="service", output_key="service"
            ),
            DataFrameStep(
                fn=process.process_service, input_key="service", output_key="service"
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Projet
    projets = ETLTask(
        task_config=TaskConfig(task_id="projets"),
        target="projets",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe, input_key="projets", output_key="projets"
            ),
            DataFrameStep(
                fn=process.process_projets, input_key="projets", output_key="projets"
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    projet_contact = ETLTask(
        task_config=TaskConfig(task_id="projet_contact"),
        target="projet_contact",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="projet_contact",
                output_key="projet_contact",
            ),
            DataFrameStep(
                fn=process.process_projet_contact,
                input_key="projet_contact",
                output_key="projet_contact",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    projet_documentation = ETLTask(
        task_config=TaskConfig(task_id="projet_documentation"),
        target="projet_documentation",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="projet_documentation",
                output_key="projet_documentation",
            ),
            DataFrameStep(
                fn=process.process_projet_documentation,
                input_key="projet_documentation",
                output_key="projet_documentation",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    projet_s3 = ETLTask(
        task_config=TaskConfig(task_id="projet_documentation"),
        target="projet_documentation",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="projet_documentation",
                output_key="projet_documentation",
            ),
            DataFrameStep(
                fn=process.process_projet_documentation,
                input_key="projet_documentation",
                output_key="projet_documentation",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    projet_s3 = ETLTask(
        task_config=TaskConfig(task_id="projet_s3"),
        target="projet_s3",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="projet_s3",
                output_key="projet_s3",
            ),
            DataFrameStep(
                fn=process.process_projet_s3,
                input_key="projet_s3",
                output_key="projet_s3",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    projet_selecteur = ETLTask(
        task_config=TaskConfig(task_id="projet_selecteur"),
        target="projet_selecteur",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="projet_selecteur",
                output_key="projet_selecteur",
            ),
            DataFrameStep(
                fn=process.process_selecteur,
                input_key="projet_selecteur",
                output_key="projet_selecteur",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Selecteur
    selecteur_source = ETLTask(
        task_config=TaskConfig(task_id="selecteur_source"),
        target="selecteur_source",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="selecteur_source",
                output_key="selecteur_source",
            ),
            DataFrameStep(
                fn=process.process_selecteur_source,
                input_key="selecteur_source",
                output_key="selecteur_source",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    selecteur_database = ETLTask(
        task_config=TaskConfig(task_id="selecteur_database"),
        target="selecteur_database",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="selecteur_database",
                output_key="selecteur_database",
            ),
            DataFrameStep(
                fn=process.process_selecteur_database,
                input_key="selecteur_database",
                output_key="selecteur_database",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    selecteur_s3 = ETLTask(
        task_config=TaskConfig(task_id="selecteur_s3"),
        target="selecteur_s3",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="selecteur_s3",
                output_key="selecteur_s3",
            ),
            DataFrameStep(
                fn=process.process_selecteur_s3,
                input_key="selecteur_s3",
                output_key="selecteur_s3",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    selecteur_column_mapping = ETLTask(
        task_config=TaskConfig(task_id="selecteur_column_mapping"),
        target="selecteur_column_mapping",
        reader=GristReaderStrategy(),
        steps=[
            DataFrameStep(
                fn=normalize_grist_dataframe,
                input_key="selecteur_column_mapping",
                output_key="selecteur_column_mapping",
            ),
            DataFrameStep(
                fn=process.process_selecteur_column_mapping,
                input_key="selecteur_column_mapping",
                output_key="selecteur_column_mapping",
            ),
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    chain(
        [
            ref_direction.create_task(),
            ref_service.create_task(),
            projets.create_task(),
            projet_contact.create_task(),
            projet_documentation.create_task(),
            projet_s3.create_task(),
            projet_selecteur.create_task(),
            selecteur_source.create_task(),
            selecteur_database.create_task(),
            selecteur_s3.create_task(),
            selecteur_column_mapping.create_task(),
        ]
    )
