from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.etl import (
    create_action_to_file_etl_task,
    create_grist_etl_task,
    create_multi_files_input_etl_task,
)
from utils.control.structures import normalize_grist_dataframe

from dags.dge.carto_rem.grist import process, actions

validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


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
    agent_contrat = create_grist_etl_task(
        selecteur="agent_contrat",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_contrat_grist,
    )
    agent_remuneration_autres_elements = create_grist_etl_task(
        selecteur="agent_remuneration_autres_elements",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_remuneration_autres_elements,
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
            agent_contrat(),
            agent_remuneration_autres_elements(),
            agent_experience_pro(),
        ]
    )


@task_group(group_id="get_db_data")
def get_db_data() -> None:
    agent_contrat_db = create_action_to_file_etl_task(
        task_id="agent_contrat_db",
        output_selecteur="agent_contrat_db",
        action_func=actions.get_agent_contrat,
        use_context=True,
    )

    chain(agent_contrat_db())


# @task_group(group_id="dataset_additionnel")
# def datasets_additionnels() -> None:
#     agent_contrat_complet = create_multi_files_input_etl_task(
#         output_selecteur="agent_contrat_complet",
#         input_selecteurs=[
#             "agent_contrat",
#             "agent_contrat_db",
#         ],
#         process_func=process.process_agent_contrat_complet,
#     )

#     chain(agent_contrat_complet())
