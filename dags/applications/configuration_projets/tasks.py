from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.control.structures import normalize_grist_dataframe
from utils.tasks.etl import create_grist_etl_task

from dags.applications.configuration_projets import process


@task_group
def process_data() -> None:
    version = "v2"
    ref_direction = create_grist_etl_task(
        selecteur="direction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_direction,
        version=version,
    )
    ref_service = create_grist_etl_task(
        selecteur="service",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_service,
        version=version,
    )
    # Projet
    projets = create_grist_etl_task(
        selecteur="projets",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet,
        version=version,
    )
    projet_contact = create_grist_etl_task(
        selecteur="projet_contact",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_contact,
        version=version,
    )
    projet_documentation = create_grist_etl_task(
        selecteur="projet_documentation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_documentation,
        version=version,
    )
    projet_s3 = create_grist_etl_task(
        selecteur="projet_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_s3,
        version=version,
    )
    projet_selecteur = create_grist_etl_task(
        selecteur="projet_selecteur",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur,
        version=version,
    )
    # Selecteur
    selecteur_source = create_grist_etl_task(
        selecteur="selecteur_source",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_source,
        version=version,
    )
    selecteur_database = create_grist_etl_task(
        selecteur="selecteur_database",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_database,
        version=version,
    )
    selecteur_s3 = create_grist_etl_task(
        selecteur="selecteur_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_s3,
        version=version,
    )

    selecteur_column_mapping = create_grist_etl_task(
        selecteur="selecteur_column_mapping",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_col_mapping,
        version=version,
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
