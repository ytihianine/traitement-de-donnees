from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.utils.process.structures import normalize_grist_dataframe
from src.common_tasks.etl import create_grist_etl_task

from src.dags.sg.dsci.experimentation_ia import process

# Création des tâches


@task_group
def referentiels() -> None:
    version = "v2"
    ref_q1_direction = create_grist_etl_task(
        selecteur="ref_q1_direction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q1_direction,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q5_domaine = create_grist_etl_task(
        selecteur="ref_q5_domaine",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q5_domaine,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q6_niveau_utilisation = create_grist_etl_task(
        selecteur="ref_q6_niveau_utilisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q6_niveau_utilisation,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q9_cas_usage = create_grist_etl_task(
        selecteur="ref_q9_cas_usage",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q9_cas_usage,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # ==============================
    # referentiels du questionnaire2
    # ==============================
    ref_q28_raisons_perte = create_grist_etl_task(
        selecteur="ref_q28_raisons_perte",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q28_raisons_perte,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q25_impact_observe = create_grist_etl_task(
        selecteur="ref_q25_impact_observe",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q25_impact_observe,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q24_impact_identifie = create_grist_etl_task(
        selecteur="ref_q24_impact_identifie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q24_impact_identifie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q23_taux_correction = create_grist_etl_task(
        selecteur="ref_q23_taux_correction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q23_taux_correction,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q22_typologie_erreurs = create_grist_etl_task(
        selecteur="ref_q22_typologie_erreurs",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q22_erreurs,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q20_autres_ia = create_grist_etl_task(
        selecteur="ref_q20_autres_ia",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q20_autres_ia,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q16_taches = create_grist_etl_task(
        selecteur="ref_q16_taches",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q16_taches,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    ref_q14_evolution_craintes = create_grist_etl_task(
        selecteur="ref_q14_evolution_craintes",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q14_evolution_craintes,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    ref_q13_facteurs_progression = create_grist_etl_task(
        selecteur="ref_q13_facteurs_progression",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q13_facteurs_progression,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    ref_q10_principaux_freins = create_grist_etl_task(
        selecteur="ref_q10_principaux_freins",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q10_principaux_freins,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    ref_q6_participation_programme = create_grist_etl_task(
        selecteur="ref_q6_participation_programme",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q6_participation_programme,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q5_formation_suivie = create_grist_etl_task(
        selecteur="ref_q5_formation_suivie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q5_formation_suivie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q3_niveau_2 = create_grist_etl_task(
        selecteur="ref_q3_niveau_2",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q3_niveau,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q7_accords = create_grist_etl_task(
        selecteur="ref_q7_accords",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q7_accords,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    # Ordre des tâches
    chain(
        [
            ref_q1_direction(),
            ref_q5_domaine(),
            ref_q6_niveau_utilisation(),
            ref_q9_cas_usage(),
            ref_q28_raisons_perte(),
            ref_q25_impact_observe(),
            ref_q24_impact_identifie(),
            ref_q23_taux_correction(),
            ref_q22_typologie_erreurs(),
            ref_q20_autres_ia(),
            ref_q16_taches(),
            ref_q14_evolution_craintes(),
            ref_q13_facteurs_progression(),
            ref_q10_principaux_freins(),
            ref_q6_participation_programme(),
            ref_q5_formation_suivie(),
            ref_q3_niveau_2(),
            ref_q7_accords(),
        ]
    )


@task_group()
def repartition() -> None:
    version = "v2"
    quota_par_entite = create_grist_etl_task(
        selecteur="quota_par_entite",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quota_entite,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # Ordre des tâches
    chain([quota_par_entite()])


@task_group
def suivi_experimentateurs() -> None:
    version = "v2"
    experimentateurs = create_grist_etl_task(
        selecteur="experimentateurs",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_experimentateurs,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # Ordre des tâches
    chain(
        [
            experimentateurs(),
        ]
    )


@task_group
def suivi_questionnaire_1() -> None:
    version = "v2"
    questionnaire_1 = create_grist_etl_task(
        selecteur="questionnaire_1",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_1,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_1_cas_usage = create_grist_etl_task(
        selecteur="questionnaire_1_cas_usage",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_1_cas_usage,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_1_besoins_accompagnement = create_grist_etl_task(
        selecteur="questionnaire_1_besoins_accompagnement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_1_besoins_accompagnement,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    # Ordre des tâches
    chain(
        [
            questionnaire_1(),
            questionnaire_1_cas_usage(),
            questionnaire_1_besoins_accompagnement(),
        ]
    )


@task_group
def suivi_questionnaire_2() -> None:
    version = "v2"
    questionnaire_2 = create_grist_etl_task(
        selecteur="questionnaire_2",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_typologie_interaction = create_grist_etl_task(
        selecteur="questionnaire2_typologie_interaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_typologie_interaction,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_formation_suivie = create_grist_etl_task(
        selecteur="questionnaire2_formation_suivie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_formation_suivie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_participation = create_grist_etl_task(
        selecteur="questionnaire2_participation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_participation,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_freins = create_grist_etl_task(
        selecteur="questionnaire2_freins",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_freins,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_facteurs_progression = create_grist_etl_task(
        selecteur="questionnaire2_facteurs_progression",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_facteurs_progression,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_taches = create_grist_etl_task(
        selecteur="questionnaire2_taches",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_taches,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_typologie_erreurs = create_grist_etl_task(
        selecteur="questionnaire2_typologie_erreurs",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_typologie_erreurs,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_impact_observe = create_grist_etl_task(
        selecteur="questionnaire2_impact_observe",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_observation_impact,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire2_impact_identifie = create_grist_etl_task(
        selecteur="questionnaire2_impact_identifie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire2_impact_identifie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    # Ordre des tâches
    chain(
        [
            questionnaire_2(),
            questionnaire2_typologie_interaction(),
            questionnaire2_formation_suivie(),
            questionnaire2_participation(),
            questionnaire2_freins(),
            questionnaire2_facteurs_progression(),
            questionnaire2_taches(),
            questionnaire2_typologie_erreurs(),
            questionnaire2_impact_observe(),
            questionnaire2_impact_identifie(),
        ]
    )
