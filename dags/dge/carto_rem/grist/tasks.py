from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.tasks.etl import (
    create_grist_etl_task,
)
from utils.control.structures import normalize_grist_dataframe

from dags.dge.carto_rem.grist import process


@task_group
def referentiels() -> None:
    ref_base_remuneration = create_grist_etl_task(
        selecteur="ref_base_remuneration",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_base_remuneration,
    )
    ref_base_revalorisation = create_grist_etl_task(
        selecteur="ref_base_revalorisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_base_revalorisation,
    )
    # ref_experience_pro = create_grist_etl_task(
    #     selecteur="ref_experience_pro",
    #     normalisation_process_func=normalize_grist_dataframe,
    #     process_func=process.process_ref_experience_pro,
    # )
    ref_niveau_diplome = create_grist_etl_task(
        selecteur="ref_niveau_diplome",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_niveau_diplome,
    )
    ref_valeur_point_indice = create_grist_etl_task(
        selecteur="ref_valeur_point_indice",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_valeur_point_indice,
    )
    ref_categorie_ecole = create_grist_etl_task(
        selecteur="ref_categorie_ecole",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_categorie_ecole,
    )
    ref_libelle_diplome = create_grist_etl_task(
        selecteur="ref_libelle_diplome",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_libelle_diplome,
    )
    ref_position = create_grist_etl_task(
        selecteur="ref_position",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_position,
    )
    ref_fonction_dge = create_grist_etl_task(
        selecteur="ref_fonction_dge",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_fonction_dge,
    )

    # ordre des tâches
    chain(
        [
            ref_base_remuneration(),
            ref_base_revalorisation(),
            # ref_experience_pro(),
            ref_niveau_diplome(),
            ref_valeur_point_indice(),
            ref_categorie_ecole(),
            ref_libelle_diplome(),
            ref_position(),
            ref_fonction_dge(),
        ]
    )


@task_group
def source_grist() -> None:
    agent_diplome = create_grist_etl_task(
        selecteur="agent_diplome",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_diplome,
    )
    agent_revalorisation = create_grist_etl_task(
        selecteur="agent_revalorisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_revalorisation,
    )
    agent_contrat_complement = create_grist_etl_task(
        selecteur="agent_contrat_complement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_contrat_complement,
    )
    agent_remuneration_complement = create_grist_etl_task(
        selecteur="agent_remuneration_complement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_remuneration_complement,
    )
    agent_experience_pro = create_grist_etl_task(
        selecteur="agent_experience_pro",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_experience_pro,
    )

    # ordre des tâches
    chain(
        [
            agent_diplome(),
            agent_revalorisation(),
            agent_contrat_complement(),
            agent_remuneration_complement(),
            agent_experience_pro(),
        ]
    )
