from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.dags.dge.carto_rem.grist import actions
from src.common_tasks.etl import (
    create_grist_etl_task,
    create_task,
)
from src._types.dags import TaskConfig, ETLStep
from src.utils.control.structures import normalize_grist_dataframe

from src.dags.dge.carto_rem.grist import process


@task_group
def referentiels() -> None:
    ref_base_remuneration = create_grist_etl_task(
        selecteur="ref_base_remuneration",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_base_remuneration,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_base_revalorisation = create_grist_etl_task(
        selecteur="ref_base_revalorisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_base_revalorisation,
        add_import_date=True,
        add_snapshot_id=True,
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
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_valeur_point_indice = create_grist_etl_task(
        selecteur="ref_valeur_point_indice",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_valeur_point_indice,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_categorie_ecole = create_grist_etl_task(
        selecteur="ref_categorie_ecole",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_categorie_ecole,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_libelle_diplome = create_grist_etl_task(
        selecteur="ref_libelle_diplome",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_libelle_diplome,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_position = create_grist_etl_task(
        selecteur="ref_position",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_position,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_fonction_dge = create_grist_etl_task(
        selecteur="ref_fonction_dge",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_fonction_dge,
        add_import_date=True,
        add_snapshot_id=True,
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
    agent = create_grist_etl_task(
        selecteur="agent",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent,
        add_import_date=True,
        add_snapshot_id=True,
    )
    agent_diplome = create_grist_etl_task(
        selecteur="agent_diplome",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_diplome,
        add_import_date=True,
        add_snapshot_id=True,
    )
    agent_revalorisation = create_grist_etl_task(
        selecteur="agent_revalorisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_revalorisation,
        add_import_date=True,
        add_snapshot_id=True,
    )
    agent_revalorisation_proposition = create_grist_etl_task(
        selecteur="agent_revalorisation_proposition",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_revalorisation_proposition,
        add_import_date=True,
        add_snapshot_id=True,
    )
    agent_contrat_complement = create_grist_etl_task(
        selecteur="agent_contrat_complement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_contrat_complement,
        add_import_date=True,
        add_snapshot_id=True,
    )
    agent_remuneration_complement = create_grist_etl_task(
        selecteur="agent_remuneration_complement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_remuneration_complement,
        add_import_date=True,
        add_snapshot_id=True,
    )
    agent_experience_pro = create_grist_etl_task(
        selecteur="agent_experience_pro",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_agent_experience_pro,
        add_import_date=True,
        add_snapshot_id=True,
    )

    # ordre des tâches
    chain(
        [
            agent(),
            agent_diplome(),
            agent_revalorisation(),
            agent_revalorisation_proposition(),
            agent_contrat_complement(),
            agent_remuneration_complement(),
            agent_experience_pro(),
        ]
    )


@task_group(group_id="load_to_grist")
def load_to_grist() -> None:
    get_agent_db = create_task(
        task_config=TaskConfig(task_id="get_agent_db"),
        output_selecteur="get_agent_db",
        steps=[ETLStep(fn=actions.get_agent_db, use_context=True)],
        add_import_date=False,
        add_snapshot_id=False,
        export_output=True,
    )

    load_agent = create_task(
        task_config=TaskConfig(task_id="load_agent"),
        output_selecteur="load_agent",
        input_selecteurs=["get_agent_db", "agent"],
        steps=[
            ETLStep(
                fn=actions.load_agent,
                read_data=True,
                use_context=True,
                kwargs={"grist_doc_selecteur": "grist_doc"},
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
        export_output=False,
    )

    chain(
        get_agent_db(),
        load_agent(),
    )
