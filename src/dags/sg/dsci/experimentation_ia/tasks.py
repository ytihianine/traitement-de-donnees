from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.utils.process.structures import normalize_grist_dataframe
from src.common_tasks.etl import create_grist_etl_task
from src.dags.sg.dsci.experimentation_ia import process

# Creation des taches


@task_group
def referentiels() -> None:
    version = "v1"

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
    # ==============================
    # referentiels du questionnaire2_bis
    # ==============================
    ref_raisons_non_utilisation = create_grist_etl_task(
        selecteur="ref_raisons_non_utilisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_raisons_non_utilisation,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # ==============================
    # Référentiels du questionnaire_3
    # ==============================
    ref_q6_formation_suivie = create_grist_etl_task(
        selecteur="ref_q6_formation_suivie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q6_formation_suivie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q7_particip_programme = create_grist_etl_task(
        selecteur="ref_q7_particip_programme",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q7_particip_programme,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q8_raisons_non_participation = create_grist_etl_task(
        selecteur="ref_q8_raisons_non_participation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q8_raisons_non_participation,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q11_leviers_progressions = create_grist_etl_task(
        selecteur="ref_q11_leviers_progressions",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q11_leviers_progressions,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q12_impacts_taches_pro = create_grist_etl_task(
        selecteur="ref_q12_impacts_taches_pro",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q12_impacts_taches_pro,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q14_taches_rebarbativ = create_grist_etl_task(
        selecteur="ref_q14_taches_rebarbativ",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q14_taches_rebarbativ,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q17_autres_outils = create_grist_etl_task(
        selecteur="ref_q17_autres_outils",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q17_autres_outils,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q17_satisfaction_autre_outil = create_grist_etl_task(
        selecteur="ref_q17_satisfaction_autre_outil",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q17_satisfaction_autre_outil,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q18_comparaisons = create_grist_etl_task(
        selecteur="ref_q18_comparaisons",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q18_comparaisons,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q19_fonctionnalites = create_grist_etl_task(
        selecteur="ref_q19_fonctionnalites",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q19_fonctionnalites,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q21_risques_identifies = create_grist_etl_task(
        selecteur="ref_q21_risques_identifies",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q21_risques_identifies,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    ref_q25_besoins = create_grist_etl_task(
        selecteur="ref_q25_besoins",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_q25_besoins,
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
            ref_raisons_non_utilisation(),
            ref_q6_formation_suivie(),
            ref_q7_particip_programme(),
            ref_q8_raisons_non_participation(),
            ref_q11_leviers_progressions(),
            ref_q12_impacts_taches_pro(),
            ref_q14_taches_rebarbativ(),
            ref_q17_autres_outils(),
            ref_q17_satisfaction_autre_outil(),
            ref_q18_comparaisons(),
            ref_q19_fonctionnalites(),
            ref_q21_risques_identifies(),
            ref_q25_besoins(),
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
    chain([experimentateurs()])


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

    # Ordre de tâches
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
    questionnaire_2_typologie_interaction = create_grist_etl_task(
        selecteur="questionnaire_2_typologie_interaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_typologie_interaction,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_formation_suivie = create_grist_etl_task(
        selecteur="questionnaire_2_formation_suivie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_formation_suivie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_participation = create_grist_etl_task(
        selecteur="questionnaire_2_participation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_participation,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_freins = create_grist_etl_task(
        selecteur="questionnaire_2_freins",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_freins,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_facteurs_progression = create_grist_etl_task(
        selecteur="questionnaire_2_facteurs_progression",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_facteurs_progression,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_taches = create_grist_etl_task(
        selecteur="questionnaire_2_taches",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_taches,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_typologie_erreurs = create_grist_etl_task(
        selecteur="questionnaire_2_typologie_erreurs",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_typologie_erreurs,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_impact_observe = create_grist_etl_task(
        selecteur="questionnaire_2_impact_observe",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_observation_impact,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_impact_identifie = create_grist_etl_task(
        selecteur="questionnaire_2_impact_identifie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_impact_identifie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )

    # Ordre des tâches
    chain(
        [
            questionnaire_2(),
            questionnaire_2_typologie_interaction(),
            questionnaire_2_formation_suivie(),
            questionnaire_2_participation(),
            questionnaire_2_freins(),
            questionnaire_2_facteurs_progression(),
            questionnaire_2_taches(),
            questionnaire_2_typologie_erreurs(),
            questionnaire_2_impact_observe(),
            questionnaire_2_impact_identifie(),
        ]
    )


@task_group
def suivi_questionnaire_2_bis() -> None:
    version = "v2"
    questionnaire_2_bis = create_grist_etl_task(
        selecteur="questionnaire_2_bis",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_bis,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_2_bis_raisons_non_utilisation = create_grist_etl_task(
        selecteur="questionnaire_2_bis_raisons_non_utilisation",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_2_bis_raisons_non_utilisation,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # Ordre des tâches
    chain(
        [
            questionnaire_2_bis(),
            questionnaire_2_bis_raisons_non_utilisation(),
        ]
    )


@task_group
def suivi_questionnaire_3() -> None:
    version = "v2"
    questionnaire_3 = create_grist_etl_task(
        selecteur="questionnaire_3",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_3_formation_suivie = create_grist_etl_task(
        selecteur="questionnaire_3_formation_suivie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3_formation_suivie,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_3_programme_rdv = create_grist_etl_task(
        selecteur="questionnaire_3_programme_rdv",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3_programme_rdv,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_3_leviers_progression = create_grist_etl_task(
        selecteur="questionnaire_3_leviers_progression",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3_leviers_progression,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_3_fonctionnalites = create_grist_etl_task(
        selecteur="questionnaire_3_fonctionnalites",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3_fonctionnalites,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_3_risques_identifies = create_grist_etl_task(
        selecteur="questionnaire_3_risques_identifies",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3_risques_identifies,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_3_besoins_prioritaires = create_grist_etl_task(
        selecteur="questionnaire_3_besoins_prioritaires",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3_besoins_prioritaires,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    questionnaire_3_besoins_moindres = create_grist_etl_task(
        selecteur="questionnaire_3_besoins_moindres",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_questionnaire_3_besoins_moindres,
        version=version,
        add_import_date=True,
        add_snapshot_id=True,
    )
    # Ordre des tâches
    chain(
        [
            questionnaire_3(),
            questionnaire_3_formation_suivie(),
            questionnaire_3_programme_rdv(),
            questionnaire_3_leviers_progression(),
            questionnaire_3_fonctionnalites(),
            questionnaire_3_risques_identifies(),
            questionnaire_3_besoins_prioritaires(),
            questionnaire_3_besoins_moindres(),
        ]
    )
