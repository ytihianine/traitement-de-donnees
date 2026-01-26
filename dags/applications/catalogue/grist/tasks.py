# TODO:
# - Update referentiels
# - Get and process catalogue from Grist
# - Get and process catalogue from Database
# - Compare both catalogues and log differences
# - Save the final catalogue to s3
# - Sync Grist and Database with the new catalogue

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from _types.dags import ALL_PARAM_PATHS
from utils.tasks.etl import (
    create_grist_etl_task,
)
from utils.control.structures import normalize_grist_dataframe

from dags.applications.catalogue.grist import process


@task_group()
def referentiels_grist() -> None:
    ref_frequency = create_grist_etl_task(
        selecteur="ref_frequency",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_frequency,
    )
    ref_contactpoint = create_grist_etl_task(
        selecteur="ref_contactpoint",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_contactpoint,
    )
    ref_people = create_grist_etl_task(
        selecteur="ref_people",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_people,
    )
    ref_geographicalcoverage = create_grist_etl_task(
        selecteur="ref_geographicalcoverage",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_geographicalcoverage,
    )
    ref_licence = create_grist_etl_task(
        selecteur="ref_licence",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_licence,
    )
    ref_service = create_grist_etl_task(
        selecteur="ref_service",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_service,
    )
    ref_format = create_grist_etl_task(
        selecteur="ref_format",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_format,
    )
    ref_organisation = create_grist_etl_task(
        selecteur="ref_organisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_organisation,
    )
    ref_informationsystem = create_grist_etl_task(
        selecteur="ref_informationsystem",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_informationsystem,
    )
    ref_theme = create_grist_etl_task(
        selecteur="ref_theme",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_theme,
    )
    ref_typedonnees = create_grist_etl_task(
        selecteur="ref_typedonnees",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_typedonnees,
    )

    """ Tasks order """
    chain(
        [
            ref_frequency(),
            ref_contactpoint(),
            ref_people(),
            ref_geographicalcoverage(),
            ref_licence(),
            ref_service(),
            ref_format(),
            ref_organisation(),
            ref_informationsystem(),
            ref_theme(),
            ref_typedonnees(),
        ]
    )


@task_group()
def source_grist() -> None:
    catalogue = create_grist_etl_task(
        selecteur="catalogue",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_catalogue,
    )
    dictionnaire = create_grist_etl_task(
        selecteur="dictionnaire",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_dictionnaire,
    )

    """ Tasks order """
    chain(
        [
            catalogue(),
            dictionnaire(),
        ]
    )


# @task_group()
# def source_database() -> None:
#     db_pg_catalog = create_action_to_file_etl_task(
#         output_selecteur="pg_catalog",
#         task_id="pg_catalog",
#         action_func=actions.extract_pg_catalog,
#     )
#     datasets = create_multi_files_input_etl_task(
#         input_selecteurs=["pg_catalog"],
#         output_selecteur="db_datasets",
#         task_id="db_datasets",
#         process_func=actions.get_db_dataset_dictionnaire,
#     )
#     datasets_dictionnaire = create_multi_files_input_etl_task(
#         input_selecteurs=["pg_catalog"],
#         output_selecteur="db_datasets_dictionnaire",
#         task_id="db_datasets_dictionnaire",
#         process_func=actions.get_db_dataset_dictionnaire,
#     )

#     """ Tasks order """
#     chain(
#         db_pg_catalog(),
#         [
#             datasets(),
#             datasets_dictionnaire(),
#         ],
#     )


# @task_group()
# def compare_catalogues() -> None:
#     pass


# @task_group()
# def sync_catalogues() -> None:
#     pass
