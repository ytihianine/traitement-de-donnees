from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task

from dags.applications.configuration_projets import config, process


@task_group
def source_grist() -> None:
    ref_direction = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_direction"),),
            output_dataset=Dataset("ref_direction"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "direction",
                ],
                cols_mapping={"id": "id_direction"},
                txt_columns=["direction"],
                custom_fn=process.process_ref_direction,
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
                custom_fn=process.process_ref_service,
            ),
        ),
        execution_options=config.execution_options,
    )
    ref_type_location = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_type_location"),),
            output_dataset=Dataset("ref_type_location"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "type_location",
                ],
                cols_mapping={"id": "id_type_location"},
                txt_columns=["type_location"],
                custom_fn=process.process_ref_type_location,
            ),
        ),
        execution_options=config.execution_options,
    )
    ref_connexion = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("ref_connexion"),),
            output_dataset=Dataset("ref_connexion"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=["id", "type_location", "conn_id"],
                cols_mapping={"id": "id_connexion", "type_location": "id_type_location"},
                txt_columns=["conn_id"],
                ref_columns=["id_type_location"],
                custom_fn=process.process_ref_connexion,
            ),
        ),
        execution_options=config.execution_options,
    )
    projet = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="projet"),),
            output_dataset=Dataset(name="projet"),
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
                custom_fn=process.process_projet,
            ),
        ),
        execution_options=config.execution_options,
    )
    projet_location = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="projet_location"),),
            output_dataset=Dataset(name="projet_location"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "projet",
                    "bucket",
                    "fs_folder",
                    "fs_folder_tmp",
                    "db_schema",
                ],
                cols_mapping={
                    "projet": "id_projet",
                },
                txt_columns=["bucket", "fs_folder", "fs_folder_tmp", "db_schema"],
                ref_columns=["id_projet"],
                custom_fn=process.process_projet_location,
            ),
        ),
        execution_options=config.execution_options,
    )
    projet_documentation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="projet_documentation"),),
            output_dataset=Dataset(name="projet_documentation"),
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
    projet_contact = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="projet_contact"),),
            output_dataset=Dataset(name="projet_contact"),
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
                int_columns=["id_contact"],
                bool_columns=["is_mail_generic"],
                custom_fn=process.process_projet_contact,
            ),
        ),
        execution_options=config.execution_options,
    )
    dataset = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="dataset"),),
            output_dataset=Dataset(name="dataset"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "projet",
                    "dataset",
                ],
                cols_mapping={
                    "id": "id_dataset",
                    "projet": "id_projet",
                },
                txt_columns=["dataset"],
                ref_columns=["id_projet"],
                custom_fn=process.process_dataset,
            ),
        ),
        execution_options=config.execution_options,
    )
    dataset_location = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="dataset_location"),),
            output_dataset=Dataset(name="dataset_location"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "projet",
                    "dataset",
                    "stage",
                    "type_location",
                    "location",
                    "conn_id",
                ],
                cols_mapping={
                    "projet": "id_projet",
                    "dataset": "id_dataset",
                    "type_location": "id_type_location",
                    "conn_id": "id_conn_id",
                },
                txt_columns=["stage", "location"],
                ref_columns=["id_projet", "id_dataset", "id_type_location", "id_conn_id"],
                custom_fn=process.process_dataset_location,
            ),
        ),
        execution_options=config.execution_options,
    )
    dataset_column_mapping = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="dataset_column_mapping"),),
            output_dataset=Dataset(name="dataset_column_mapping"),
            operation=partial(
                generic_grist_processing,
                cols_to_keep=[
                    "id",
                    "projet",
                    "dataset",
                    "colname_source",
                    "colname_dest",
                    "to_keep",
                    "date_archivage",
                ],
                cols_mapping={
                    "id": "id_col_mapping",
                    "projet": "id_projet",
                    "dataset": "id_dataset",
                },
                txt_columns=["colname_source", "colname_dest"],
                ref_columns=["id_projet", "id_dataset"],
                bool_columns=["to_keep"],
                date_columns=["date_archivage"],
                custom_fn=process.process_dataset_column_mapping,
            ),
        ),
        execution_options=config.execution_options,
    )

    chain(
        [
            ref_direction(),
            ref_service(),
            ref_type_location(),
            ref_connexion(),
            projet(),
            projet_location(),
            projet_contact(),
            projet_documentation(),
            dataset(),
            dataset_location(),
            dataset_column_mapping(),
        ]
    )


@task_group
def projet_dimension_tables() -> None:
    dim_projet = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset(name="projet"),
                Dataset(name="ref_direction"),
                Dataset(name="ref_service"),
                Dataset(name="projet_location"),
            ),
            output_dataset=Dataset(name="dim_projet"),
            operation=process.process_dim_projet,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )
    dim_projet_contact = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="projet"), Dataset(name="projet_contact")),
            output_dataset=Dataset(name="dim_projet_contact"),
            operation=process.process_dim_projet_contact,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )
    dim_projet_documentation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="projet"), Dataset(name="projet_documentation")),
            output_dataset=Dataset(name="dim_projet_documentation"),
            operation=process.process_dim_projet_documentation,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )

    chain(
        [
            dim_projet(),
            dim_projet_contact(),
            dim_projet_documentation(),
        ]
    )


@task_group
def dataset_dimension_tables() -> None:
    dim_dataset = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset(name="projet"),
                Dataset(name="ref_direction"),
                Dataset(name="ref_service"),
                Dataset(name="dataset"),
            ),
            output_dataset=Dataset(name="dim_dataset"),
            operation=process.process_dim_dataset,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )
    dim_dataset_location = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset(name="dim_dataset"),
                Dataset(name="dataset_location"),
                Dataset(name="ref_type_location"),
                Dataset(name="ref_connexion"),
            ),
            output_dataset=Dataset(name="dim_dataset_location"),
            operation=process.process_dim_dataset_location,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )
    dim_dataset_cols_mapping = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset(name="projet"), Dataset(name="dataset"), Dataset(name="dataset_column_mapping")),
            output_dataset=Dataset(name="dim_dataset_column_mapping"),
            operation=process.process_dim_dataset_column_mapping,
            use_input_results_as_operation_args=True,
        ),
        execution_options=config.execution_options,
    )

    chain(
        [
            dim_dataset(),
            dim_dataset_location(),
            dim_dataset_cols_mapping(),
        ]
    )
