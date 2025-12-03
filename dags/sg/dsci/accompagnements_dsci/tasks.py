from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.etl import create_grist_etl_task

from dags.sg.dsci.accompagnements_dsci import process
from utils.tasks.validation import create_validate_params_task


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


# Création des tâches
@task_group
def referentiels() -> None:
    ref_bureau = create_grist_etl_task(
        selecteur="ref_bureau",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_bureau,
    )
    ref_certification = create_grist_etl_task(
        selecteur="ref_certification",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_certification,
    )
    ref_competence_particuliere = create_grist_etl_task(
        selecteur="ref_competence_particuliere",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_competence_particuliere,
    )
    ref_direction = create_grist_etl_task(
        selecteur="ref_direction",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_direction,
    )
    ref_profil_correspondant = create_grist_etl_task(
        selecteur="ref_profil_correspondant",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_profil_correspondant,
    )
    ref_qualite_service = create_grist_etl_task(
        selecteur="ref_qualite_service",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_qualite_service,
    )
    ref_region = create_grist_etl_task(
        selecteur="ref_region",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_region,
    )
    ref_semainier = create_grist_etl_task(
        selecteur="ref_semainier",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_semainier,
    )
    ref_typologie_accompagnement = create_grist_etl_task(
        selecteur="ref_typologie_accompagnement",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_typologie_accompagnement,
    )
    ref_pole = create_grist_etl_task(
        selecteur="ref_pole",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_pole,
    )
    ref_type_accompagnement = create_grist_etl_task(
        selecteur="ref_type_accompagnement",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_type_accompagnement,
    )

    # Ordre des tâches
    chain(
        [
            ref_bureau(),
            ref_certification(),
            ref_competence_particuliere(),
            ref_direction(),
            ref_profil_correspondant(),
            ref_qualite_service(),
            ref_region(),
            ref_semainier(),
            ref_typologie_accompagnement(),
            ref_pole(),
            ref_type_accompagnement(),
        ],
    )


@task_group
def bilaterales() -> None:
    bilaterale = create_grist_etl_task(
        selecteur="bilaterale",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_bilaterale,
    )
    bilaterale_remontee = create_grist_etl_task(
        selecteur="bilaterale_remontee",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_bilaterale_remontee,
    )
    # Ordre des tâches
    chain([bilaterale(), bilaterale_remontee()])


@task_group
def correspondant() -> None:
    correspondant = create_grist_etl_task(
        selecteur="correspondant",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_correspondant,
    )
    correspondant_profil = create_grist_etl_task(
        selecteur="correspondant_profil",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_correspondant_profil,
    )
    # correspondant_certification = create_grist_etl_task(
    #     selecteur="correspondant_certification",
    #     process_func=process.process_correspondant_certification,
    # )

    # Ordre des tâches
    chain([correspondant(), correspondant_profil()])


@task_group
def mission_innovation() -> None:
    accompagnement_mi = create_grist_etl_task(
        selecteur="accompagnement_mi",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_accompagnement_mi,
    )
    accompagnement_mi_satisfaction = create_grist_etl_task(
        selecteur="accompagnement_mi_satisfaction",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_accompagnement_mi_satisfaction,
    )

    # Ordre des tâches
    chain([accompagnement_mi(), accompagnement_mi_satisfaction()])
