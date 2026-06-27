from functools import partial
from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.common_tasks.grist import generic_grist_processing
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"id": "id_direction"},
                    cols_to_keep=[
                        "id_direction",
                        "direction",
                    ],
                    txt_columns=["direction"],
                    custom_fn=process.process_direction,
                ),
                input_key="direction",
                output_key="direction",
            )
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"direction": "id_direction"},
                    cols_to_keep=[
                        "id",
                        "id_direction",
                        "service",
                    ],
                    txt_columns=["service"],
                    ref_columns=["id_direction"],
                    custom_fn=process.process_service,
                ),
                input_key="service",
                output_key="service",
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_projet",
                        "direction": "id_direction",
                        "service": "id_service",
                    },
                    cols_to_keep=[
                        "id_projet",
                        "projet",
                        "id_direction",
                        "id_service",
                    ],
                    txt_columns=["projet"],
                    ref_columns=["id_direction", "id_service"],
                    custom_fn=process.process_projets,
                ),
                input_key="projets",
                output_key="projets",
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_contact",
                        "projet": "id_projet",
                    },
                    cols_to_keep=[
                        "id_contact",
                        "id_projet",
                        "contact_mail",
                        "is_mail_generic",
                    ],
                    txt_columns=["contact_mail"],
                    ref_columns=["id_projet"],
                    bool_columns=["is_mail_generic"],
                    custom_fn=process.process_projet_contact,
                ),
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "projet": "id_projet",
                    },
                    cols_to_keep=[
                        "id_projet",
                        "type_documentation",
                        "lien",
                    ],
                    txt_columns=["type_documentation", "lien"],
                    ref_columns=["id_projet"],
                    bool_columns=["is_mail_generic"],
                    custom_fn=process.process_projet_documentation,
                ),
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "projet": "id_projet",
                    },
                    cols_to_keep=[
                        "id_projet",
                        "bucket",
                        "key",
                        "key_tmp",
                    ],
                    txt_columns=["bucket", "key", "key_tmp"],
                    ref_columns=["id_projet"],
                    custom_fn=process.process_projet_s3,
                ),
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_selecteur",
                        "projet": "id_projet",
                        "type_de_selecteur": "type_selecteur",
                        "selecteur": "selecteur",
                    },
                    cols_to_keep=[
                        "id_selecteur",
                        "id_projet",
                        "type_selecteur",
                        "selecteur",
                    ],
                    txt_columns=["selecteur"],
                    ref_columns=["id_projet"],
                    custom_fn=process.process_projet_selecteur,
                ),
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "projet": "id_projet",
                        "type": "type_source",
                        "selecteur": "id_selecteur",
                    },
                    cols_to_keep=[
                        "projet",
                        "type_source",
                        "id_selecteur",
                        "id_source",
                    ],
                    txt_columns=["type_source", "id_source"],
                    ref_columns=["id_projet", "id_selecteur"],
                    custom_fn=process.process_selecteur_source,
                ),
                input_key="selecteur_source",
                output_key="selecteur_source",
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "projet": "id_projet",
                        "selecteur": "id_selecteur",
                    },
                    cols_to_keep=[
                        "id_projet",
                        "id_selecteur",
                        "filename",
                        "key",
                    ],
                    txt_columns=["filename", "key"],
                    ref_columns=["id_projet", "id_selecteur"],
                    custom_fn=process.process_selecteur_s3,
                ),
                input_key="selecteur_s3",
                output_key="selecteur_s3",
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "projet": "id_projet",
                        "selecteur": "id_selecteur",
                    },
                    cols_to_keep=[
                        "id_projet",
                        "id_selecteur",
                        "tbl_name",
                    ],
                    txt_columns=["tbl_name"],
                    ref_columns=["id_projet", "id_selecteur"],
                    custom_fn=process.process_selecteur_database,
                ),
                input_key="selecteur_database",
                output_key="selecteur_database",
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
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_col_mapping",
                        "projet": "id_projet",
                        "selecteur": "id_selecteur",
                    },
                    cols_to_keep=[
                        "id",
                        "projet",
                        "selecteur",
                        "colname_source",
                        "colname_dest",
                        "to_keep",
                        "date_archivage",
                    ],
                    txt_columns=["colname_source", "colname_dest"],
                    ref_columns=["id_projet", "id_selecteur"],
                    bool_columns=["to_keep"],
                    date_columns=["date_archivage"],
                    custom_fn=process.process_selecteur_column_mapping,
                ),
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
