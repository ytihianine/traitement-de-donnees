from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.control.structures import normalize_grist_dataframe
from utils.tasks.etl import create_grist_etl_task

from dags.applications.configuration_projets import process


@task_group
def process_data() -> None:
    ref_direction = create_grist_etl_task(
        selecteur="direction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_direction,
        version="v2",
    )
    ref_service = create_grist_etl_task(
        selecteur="service",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_service,
        version="v2",
    )
    # Projet
    projets = create_grist_etl_task(
        selecteur="projets",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet,
        version="v2",
    )
    projet_contact = create_grist_etl_task(
        selecteur="projet_contact",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_contact,
        version="v2",
    )
    projet_documentation = create_grist_etl_task(
        selecteur="projet_documentation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_documentation,
        version="v2",
    )
    projet_s3 = create_grist_etl_task(
        selecteur="projet_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_s3,
        version="v2",
    )
    projet_selecteur = create_grist_etl_task(
        selecteur="projet_selecteur",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur,
        version="v2",
    )
    # Selecteur
    selecteur_source = create_grist_etl_task(
        selecteur="selecteur_source",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_source,
        version="v2",
    )
    selecteur_database = create_grist_etl_task(
        selecteur="selecteur_database",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_database,
        version="v2",
    )
    selecteur_s3 = create_grist_etl_task(
        selecteur="selecteur_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_s3,
        version="v2",
    )

    selecteur_column_mapping = create_grist_etl_task(
        selecteur="selecteur_column_mapping",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_col_mapping,
        version="v2",
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
