from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.control.structures import normalize_grist_dataframe
from utils.tasks.validation import create_validate_params_task
from types.dags import ALL_PARAM_PATHS
from utils.tasks.etl import create_grist_etl_task

from dags.applications.configuration_projets import process


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group
def process_data() -> None:
    ref_direction = create_grist_etl_task(
        selecteur="direction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_direction,
    )
    ref_service = create_grist_etl_task(
        selecteur="service",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_service,
    )
    # Projet
    projets = create_grist_etl_task(
        selecteur="projets",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet,
    )
    projet_contact = create_grist_etl_task(
        selecteur="projet_contact",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_contact,
    )
    projet_documentation = create_grist_etl_task(
        selecteur="projet_documentation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_documentation,
    )
    projet_s3 = create_grist_etl_task(
        selecteur="projet_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_projet_s3,
    )
    projet_selecteur = create_grist_etl_task(
        selecteur="projet_selecteur",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur,
    )
    # Selecteur
    selecteur = create_grist_etl_task(
        selecteur="selecteur",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur,
    )
    selecteur_source = create_grist_etl_task(
        selecteur="selecteur_source",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_source,
    )
    selecteur_database = create_grist_etl_task(
        selecteur="selecteur_database",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_database,
    )
    selecteur_s3 = create_grist_etl_task(
        selecteur="selecteur_s3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_selecteur_s3,
    )

    selecteur_column_mapping = create_grist_etl_task(
        selecteur="selecteur_column_mapping",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_col_mapping,
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
            selecteur(),
            selecteur_source(),
            selecteur_database(),
            selecteur_s3(),
            selecteur_column_mapping(),
        ]
    )
