from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.utils.process.structures import normalize_grist_dataframe
from src.common_tasks.etl import create_grist_etl_task

from dags.applications.configuration_projets import process


@task_group
def process_data() -> None:
    version = "v2"
    ref_direction = create_grist_etl_task(
        selecteur="direction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_direction,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_service = create_grist_etl_task(
        selecteur="service",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_service,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # Projet
    projets = create_grist_etl_task(
        selecteur="projets",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    projet_contact = create_grist_etl_task(
        selecteur="projet_contact",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_contact,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    projet_documentation = create_grist_etl_task(
        selecteur="projet_documentation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_documentation,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    projet_s3 = create_grist_etl_task(
        selecteur="projet_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_s3,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    projet_selecteur = create_grist_etl_task(
        selecteur="projet_selecteur",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # Selecteur
    selecteur_source = create_grist_etl_task(
        selecteur="selecteur_source",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_source,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    selecteur_database = create_grist_etl_task(
        selecteur="selecteur_database",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_database,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    selecteur_s3 = create_grist_etl_task(
        selecteur="selecteur_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_s3,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    selecteur_column_mapping = create_grist_etl_task(
        selecteur="selecteur_column_mapping",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_col_mapping,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
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
