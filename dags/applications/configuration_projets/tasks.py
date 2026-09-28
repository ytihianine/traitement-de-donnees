from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.applications.configuration_projets import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


@task_group
def process_data() -> None:
    ref_direction = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("direction"),),
            output_dataset=Dataset("direction"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "direction",
                ],
                cols_mapping={"id": "id_direction"},
                txt_columns=["direction"],
                custom_fn=process.process_direction,
            ),
        ),
        execution_options=config.execution_options,
    )
    ref_service = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_service"),),
            output_dataset=Dataset("ref_service"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "direction",
                    "service",
                ],
                cols_mapping={"direction": "id_direction", "id": "id_service"},
                txt_columns=["service"],
                ref_columns=["id_direction"],
                custom_fn=process.process_service,
            ),
        ),
        execution_options=config.execution_options,
    )
    # Projet
    projets = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("projets"),),
            output_dataset=Dataset("projets"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "projet",
                    "direction",
                    "service",
                ],
                cols_mapping={
                    "id": "id_projet",
                    "direction": "id_direction",
                    "service": "id_service",
                },
                txt_columns=["projet"],
                ref_columns=["id_direction", "id_service"],
                custom_fn=process.process_projets,
            ),
        ),
        execution_options=config.execution_options,
    )
    projet_contact = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("projet_contact"),),
            output_dataset=Dataset("projet_contact"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "projet",
                    "contact_mail",
                    "is_mail_generic",
                ],
                cols_mapping={
                    "id": "id_contact",
                    "projet": "id_projet",
                },
                txt_columns=["contact_mail"],
                ref_columns=["id_projet"],
                bool_columns=["is_mail_generic"],
                custom_fn=process.process_projet_contact,
            ),
        ),
        execution_options=config.execution_options,
    )
    projet_documentation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("projet_documentation"),),
            output_dataset=Dataset("projet_documentation"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "projet",
                    "type_documentation",
                    "lien",
                ],
                cols_mapping={
                    "projet": "id_projet",
                },
                txt_columns=["type_documentation", "lien"],
                ref_columns=["id_projet"],
                custom_fn=process.process_projet_documentation,
            ),
        ),
        execution_options=config.execution_options,
    )
    projet_s3 = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("projet_s3"),),
            output_dataset=Dataset("projet_s3"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "projet",
                    "bucket",
                    "key",
                    "key_tmp",
                ],
                cols_mapping={
                    "projet": "id_projet",
                },
                txt_columns=["bucket", "key", "key_tmp"],
                ref_columns=["id_projet"],
                custom_fn=process.process_projet_s3,
            ),
        ),
        execution_options=config.execution_options,
    )
    projet_selecteur = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("projet_selecteur"),),
            output_dataset=Dataset("projet_selecteur"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "projet",
                    "type_de_selecteur",
                    "selecteur",
                ],
                cols_mapping={
                    "id": "id_selecteur",
                    "projet": "id_projet",
                    "type_de_selecteur": "type_selecteur",
                },
                txt_columns=["selecteur", "type_selecteur"],
                ref_columns=["id_projet"],
                custom_fn=process.process_projet_selecteur,
            ),
        ),
        execution_options=config.execution_options,
    )
    # Selecteur
    selecteur_source = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("selecteur_source"),),
            output_dataset=Dataset("selecteur_source"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "projet",
                    "type",
                    "selecteur",
                    "id_source",
                ],
                cols_mapping={
                    "projet": "id_projet",
                    "selecteur": "id_selecteur",
                    "type": "type_location",
                },
                txt_columns=["type_location", "id_source"],
                ref_columns=["id_projet", "id_selecteur"],
                custom_fn=process.process_selecteur_source,
            ),
        ),
        execution_options=config.execution_options,
    )
    selecteur_s3 = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("selecteur_s3"),),
            output_dataset=Dataset("selecteur_s3"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "projet",
                    "selecteur",
                    "filename",
                    "key",
                ],
                cols_mapping={
                    "projet": "id_projet",
                    "selecteur": "id_selecteur",
                },
                txt_columns=["filename", "key"],
                ref_columns=["id_projet", "id_selecteur"],
                custom_fn=process.process_selecteur_s3,
            ),
        ),
        execution_options=config.execution_options,
    )

    selecteur_database = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("selecteur_database"),),
            output_dataset=Dataset("selecteur_database"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "projet",
                    "selecteur",
                    "tbl_name",
                ],
                cols_mapping={
                    "projet": "id_projet",
                    "selecteur": "id_selecteur",
                },
                txt_columns=["tbl_name"],
                ref_columns=["id_projet", "id_selecteur"],
                custom_fn=process.process_selecteur_database,
            ),
        ),
        execution_options=config.execution_options,
    )

    selecteur_column_mapping = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("selecteur_column_mapping"),),
            output_dataset=Dataset("selecteur_column_mapping"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "projet",
                    "selecteur",
                    "colname_source",
                    "colname_dest",
                    "to_keep",
                    "date_archivage",
                ],
                cols_mapping={
                    "id": "id_col_mapping",
                    "projet": "id_projet",
                    "selecteur": "id_selecteur",
                },
                txt_columns=["colname_source", "colname_dest"],
                ref_columns=["id_projet", "id_selecteur"],
                bool_columns=["to_keep"],
                date_columns=["date_archivage"],
                custom_fn=process.process_selecteur_column_mapping,
            ),
        ),
        execution_options=config.execution_options,
    )

    chain(
        [
            ref_direction(),
            ref_service(),
            projets(),
            projet_contact(),
            projet_documentation(),
            projet_s3(),
            projet_selecteur(),
            selecteur_source(),
            selecteur_database(),
            selecteur_s3(),
            selecteur_column_mapping(),
        ]
    )
