from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.experimentation_ia import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


def _grist_pipeline(dataset_name: str, custom_fn, **grist_kwargs) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=partial(
            generic_grist_processing,
            custom_fn=custom_fn,
            **grist_kwargs,
        ),
    )


@task_group()
def referentiels() -> None:
    ref_q1_direction = create_task(
        pipeline=_grist_pipeline(
            "ref_q1_direction",
            process.process_ref_q1_direction,
            txt_columns=["direction"],
        ),
        execution_options=config.execution_options,
    )
    ref_q5_domaine = create_task(
        pipeline=_grist_pipeline(
            "ref_q5_domaine",
            process.process_ref_q5_domaine,
            txt_columns=["domaine"],
        ),
        execution_options=config.execution_options,
    )
    ref_q6_niveau_utilisation = create_task(
        pipeline=_grist_pipeline(
            "ref_q6_niveau_utilisation",
            process.process_ref_q6_niveau_utilisation,
            cols_to_keep=["id", "niveau_d_appropriation"],
            txt_columns=["niveau_d_appropriation"],
        ),
        execution_options=config.execution_options,
    )
    ref_q9_cas_usage = create_task(
        pipeline=_grist_pipeline(
            "ref_q9_cas_usage",
            process.process_ref_q9_cas_usage,
            txt_columns=["cas_d_usage"],
        ),
        execution_options=config.execution_options,
    )
    # ==============================
    # referentiels du questionnaire2
    # ==============================
    ref_q28_raisons_perte = create_task(
        pipeline=_grist_pipeline(
            "ref_q28_raisons_perte",
            process.process_ref_q28_raisons_perte,
            txt_columns=["raisons"],
        ),
        execution_options=config.execution_options,
    )
    ref_q25_impact_observe = create_task(
        pipeline=_grist_pipeline(
            "ref_q25_impact_observe",
            process.process_ref_q25_impact_observe,
            txt_columns=["observation"],
        ),
        execution_options=config.execution_options,
    )
    ref_q24_impact_identifie = create_task(
        pipeline=_grist_pipeline(
            "ref_q24_impact_identifie",
            process.process_ref_q24_impact_identifie,
            txt_columns=["impacts"],
        ),
        execution_options=config.execution_options,
    )
    ref_q23_taux_correction = create_task(
        pipeline=_grist_pipeline(
            "ref_q23_taux_correction",
            process.process_ref_q23_taux_correction,
            cols_to_keep=["id", "taux_de_correction"],
            txt_columns=["taux_de_correction"],
        ),
        execution_options=config.execution_options,
    )
    ref_q22_typologie_erreurs = create_task(
        pipeline=_grist_pipeline(
            "ref_q22_typologie_erreurs",
            process.process_ref_q22_typologie_erreurs,
            txt_columns=["erreurs"],
        ),
        execution_options=config.execution_options,
    )
    ref_q20_autres_ia = create_task(
        pipeline=_grist_pipeline(
            "ref_q20_autres_ia",
            process.process_ref_q20_autres_ia,
            txt_columns=["comparaisons"],
        ),
        execution_options=config.execution_options,
    )
    ref_q16_taches = create_task(
        pipeline=_grist_pipeline(
            "ref_q16_taches",
            process.process_ref_q16_taches,
            txt_columns=["taches"],
        ),
        execution_options=config.execution_options,
    )
    ref_q14_evolution_craintes = create_task(
        pipeline=_grist_pipeline(
            "ref_q14_evolution_craintes",
            process.process_ref_q14_evolution_craintes,
            txt_columns=["evolutions"],
        ),
        execution_options=config.execution_options,
    )
    ref_q13_facteurs_progression = create_task(
        pipeline=_grist_pipeline(
            "ref_q13_facteurs_progression",
            process.process_ref_q13_facteurs_progression,
            txt_columns=["facteurs"],
        ),
        execution_options=config.execution_options,
    )
    ref_q10_principaux_freins = create_task(
        pipeline=_grist_pipeline(
            "ref_q10_principaux_freins",
            process.process_ref_q10_principaux_freins,
            txt_columns=["freins"],
        ),
        execution_options=config.execution_options,
    )
    ref_q6_participation_programme = create_task(
        pipeline=_grist_pipeline(
            "ref_q6_participation_programme",
            process.process_ref_q6_participation_programme,
            txt_columns=["participation"],
        ),
        execution_options=config.execution_options,
    )
    ref_q5_formation_suivie = create_task(
        pipeline=_grist_pipeline(
            "ref_q5_formation_suivie",
            process.process_ref_q5_formation_suivie,
            txt_columns=["formation_suivie"],
        ),
        execution_options=config.execution_options,
    )
    ref_q3_niveau_2 = create_task(
        pipeline=_grist_pipeline(
            "ref_q3_niveau_2",
            process.process_ref_q3_niveau,
            txt_columns=["niveau"],
        ),
        execution_options=config.execution_options,
    )
    ref_q7_accords = create_task(
        pipeline=_grist_pipeline(
            "ref_q7_accords",
            process.process_ref_q7_accords,
            txt_columns=["reponses"],
        ),
        execution_options=config.execution_options,
    )
    # ==============================
    # referentiels du questionnaire2_bis
    # ==============================
    ref_raisons_non_utilisation = create_task(
        pipeline=_grist_pipeline(
            "ref_raisons_non_utilisation",
            process.process_ref_raisons_non_utilisation,
            txt_columns=["raisons"],
        ),
        execution_options=config.execution_options,
    )
    # ==============================
    # Référentiels du questionnaire_3
    # ==============================
    ref_q6_formation_suivie = create_task(
        pipeline=_grist_pipeline(
            "ref_q6_formation_suivie",
            process.process_ref_q6_formation_suivie,
            txt_columns=["formation"],
        ),
        execution_options=config.execution_options,
    )
    ref_q7_particip_programme = create_task(
        pipeline=_grist_pipeline(
            "ref_q7_particip_programme",
            process.process_ref_q7_particip_programme,
            txt_columns=["participation"],
        ),
        execution_options=config.execution_options,
    )
    ref_q8_raisons_non_participation = create_task(
        pipeline=_grist_pipeline(
            "ref_q8_raisons_non_participation",
            process.process_ref_q8_raisons_non_participation,
            txt_columns=["raisons"],
        ),
        execution_options=config.execution_options,
    )
    ref_q11_leviers_progressions = create_task(
        pipeline=_grist_pipeline(
            "ref_q11_leviers_progressions",
            process.process_ref_q11_leviers_progressions,
            txt_columns=["leviers"],
        ),
        execution_options=config.execution_options,
    )
    ref_q12_impacts_taches_pro = create_task(
        pipeline=_grist_pipeline(
            "ref_q12_impacts_taches_pro",
            process.process_ref_q12_impacts_taches_pro,
            txt_columns=["impacts"],
        ),
        execution_options=config.execution_options,
    )
    ref_q14_taches_rebarbativ = create_task(
        pipeline=_grist_pipeline(
            "ref_q14_taches_rebarbativ",
            process.process_ref_q14_taches_rebarbativ,
            txt_columns=["taches_rebarbatives"],
        ),
        execution_options=config.execution_options,
    )
    ref_q17_autres_outils = create_task(
        pipeline=_grist_pipeline(
            "ref_q17_autres_outils",
            process.process_ref_q17_autres_outils,
            cols_to_keep=["id", "autres_outils"],
            txt_columns=["autres_outils"],
        ),
        execution_options=config.execution_options,
    )
    ref_q17_satisfaction_autre_outil = create_task(
        pipeline=_grist_pipeline(
            "ref_q17_satisfaction_autre_outil",
            process.process_ref_q17_satisfaction_autre_outil,
            cols_to_keep=["id", "satisfaction_autres_outils"],
            txt_columns=["satisfaction_autres_outils"],
        ),
        execution_options=config.execution_options,
    )
    ref_q18_comparaisons = create_task(
        pipeline=_grist_pipeline(
            "ref_q18_comparaisons",
            process.process_ref_q18_comparaisons,
            txt_columns=["comparaisons"],
        ),
        execution_options=config.execution_options,
    )
    ref_q19_fonctionnalites = create_task(
        pipeline=_grist_pipeline(
            "ref_q19_fonctionnalites",
            process.process_ref_q19_fonctionnalites,
            txt_columns=["fonctionnalites"],
        ),
        execution_options=config.execution_options,
    )
    ref_q21_risques_identifies = create_task(
        pipeline=_grist_pipeline(
            "ref_q21_risques_identifies",
            process.process_ref_q21_risques_identifies,
            txt_columns=["risques"],
        ),
        execution_options=config.execution_options,
    )
    ref_q25_besoins = create_task(
        pipeline=_grist_pipeline(
            "ref_q25_besoins",
            process.process_ref_q25_besoins,
            txt_columns=["besoins"],
        ),
        execution_options=config.execution_options,
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
    quota_par_entite = create_task(
        pipeline=_grist_pipeline(
            "quota_par_entite",
            process.process_quota_par_entite,
            cols_to_keep=[
                "experimentation_demarree",
                "entite",
                "nbre_d_acces_previsionnels",
                "nb_acces_demande",
                "code",
                "nbre_de_connexion_effective_au_05_03_2026",
                "nb_de_reponses_au_questionnaire",
                "nb_reponse_q2",
                "nb_reponse_q3",
                "relance_dsci",
                "appel_a_candidature_dsci",
                "referent_ia",
                "courriel",
            ],
            cols_mapping={"nbre_de_connexion_effective_au_05_03_2026": "nbre_connexion_effective"},
            txt_columns=[
                "code",
                "relance_dsci",
                "appel_a_candidature_dsci",
                "referent_ia",
                "courriel",
            ],
            num_columns=[
                "nbre_d_acces_previsionnels",
                "nbre_connexion_effective",
            ],
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain([quota_par_entite()])


@task_group()
def suivi_experimentateurs() -> None:
    experimentateurs = create_task(
        pipeline=_grist_pipeline(
            "experimentateurs",
            process.process_experimentateurs,
            cols_to_keep=[
                "no_id",
                "entite",
                "parti",
                "courriel",
                "courriel_corrige",
                "connecte_",
                "reponse_au_questionnaire_1",
                "reponse_au_questionnaire_2",
                "reponse_au_questionnaire_3",
            ],
            txt_columns=[
                "no_id",
                "parti",
                "courriel",
                "courriel_corrige",
            ],
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain([experimentateurs()])


@task_group()
def suivi_questionnaire_1() -> None:
    questionnaire_1 = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_1",
            process.process_questionnaire_1,
            cols_to_keep=[
                "no_id",
                "direction",
                "tranche_age",
                "categorie_emploi",
                "statut",
                "domaine_professionnel",
                "metier",
                "situation_d_encadrement",
                "autres_experimentateurs",
                "niveau_d_utilisation_ia",
                "usage_ia_perso_avant_expe",
                "usage_ia_pro_avant_expe",
                "craintes_usage_ia_pro",
                "raisons_des_craintes",
                "attentes_experimentation",
                "autres_cas_usage_transverse",
                "cas_d_usage_metier",
                "formation_suivie_usage_ia_",
                "autre_formation_suivie",
                "autre_besoin_accompagnement",
                "besoin_acculturation_encadrement",
            ],
            cols_mapping={
                "direction": "id_direction",
                "domaine_professionnel": "id_domaine_professionnel",
                "niveau_d_utilisation_ia": "id_niveau_d_utilisation_ia",
            },
            txt_columns=[
                "no_id",
                "metier",
                "raisons_des_craintes",
                "attentes_experimentation",
                "cas_d_usage_metier",
                "autres_cas_usage_transverse",
                "autre_formation_suivie",
                "autre_besoin_accompagnement",
            ],
            ref_columns=[
                "id_direction",
                "id_domaine_professionnel",
                "id_niveau_d_utilisation_ia",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_1_cas_usage = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_1_cas_usage",
            process.process_questionnaire_1_cas_usage,
            cols_to_keep=["no_id", "cas_d_usage_envisages"],
            cols_mapping={
                "cas_d_usage_envisages": "id_cas_d_usage_envisages",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_1_besoins_accompagnement = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_1_besoins_accompagnement",
            process.process_questionnaire_1_besoins_accompagnement,
            cols_to_keep=["no_id", "besoin_accompagnement"],
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )

    # Ordre de tâches
    chain(
        [
            questionnaire_1(),
            questionnaire_1_cas_usage(),
            questionnaire_1_besoins_accompagnement(),
        ]
    )


@task_group()
def suivi_questionnaire_2() -> None:
    questionnaire_2 = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2",
            process.process_questionnaire_2,
            cols_to_keep=[
                "no_id",
                "autres_types_d_interactions",
                "niveau_d_usage_ia_post_expe_",
                "frequence_d_usage_assistant_ia",
                "autres_formation_ia",
                "raison_non_participation_rdv",
                "autre_besoin_accompagnement",
                "apprentissage_assistant_ia_ressenti_",
                "difficultes_techniques_rencontrees2",
                "autres_difficultes",
                "autres_taches_realisees",
                "autres_freins",
                "recommandation_collegues_mef",
                "sensation_montee_en_competences",
                "autres_sources_de_progression",
                "evolution_des_craintes_initiales",
                "utilite_metier_mef",
                "decouverte_d_usages_inattendus",
                "les_usages_inattendus",
                "mode_de_decouverte_usages",
                "autre_mode_de_decouverte",
                "diminution_d_usage_ia_non_souveraines",
                "comparaison_autres_ia",
                "frequence_des_erreurs",
                "autres_types_d_erreurs",
                "cas_usage_principal_teste",
                "temps_economise_par_semaine",
                "cu1_nombre_echanges_moyens_affinage_reponse",
                "taux_moyen_de_correction_rep_assistant",
                "pertinence_assistant_ia",
                "commentaires",
                "deuxieme_cas_d_usage_teste",
                "cu2_temps_economise_par_semaine",
                "cu2_nombre_echanges_moyens",
                "cu2_taux_moyen_de_correction_assistant",
                "cu2_pertinence_assistant_ia",
                "commentaires2",
                "troisieme_cas_d_usage",
                "cu3_temps_economise_par_semaine",
                "cu3_nombre_echanges_moyens_affinage_reponse",
                "cu3_taux_moyen_de_correction_assistant",
                "cu3_pertinence_assistant_ia",
                "commentaires3",
                "autres_impacts_identifies",
                "autres_impacts_observes",
                "impact_sur_le_temps_de_travail",
                "estimation_globale_gain_de_temps",
                "raisons_perte_de_temps",
                "autres_raisons",
                "ia_favorise_relations_humaines_",
            ],
            cols_mapping={
                "niveau_d_usage_ia_post_expe_": "id_niveau_d_usage_ia_post_expe_",
                "recommandation_collegues_mef": "id_recommandation_collegues_mef",
                "sensation_montee_en_competences": "id_sensation_montee_en_competences",
                "evolution_des_craintes_initiales": "id_evolution_des_craintes_initiales",
                "utilite_metier_mef": "id_utilite_metier_mef",
                "diminution_d_usage_ia_non_souveraines": "id_diminution_d_usage_ia_non_souveraines",
                "comparaison_autres_ia": "id_comparaison_autres_ia",
                "taux_moyen_de_correction_rep_assistant": "id_taux_moyen_de_correction_rep_assistant",
                "cu2_taux_moyen_de_correction_assistant": "id_cu2_taux_moyen_de_correction_rep_assistant",
                "cu3_taux_moyen_de_correction_assistant": "id_cu3_taux_moyen_de_correction_rep_assistant",
                "raisons_perte_de_temps": "id_raisons_perte_de_temps",
                "ia_favorise_relations_humaines_": "id_ia_favorise_relations_humaines_",
            },
            txt_columns=[
                "no_id",
                "autres_types_d_interactions",
                "autres_formation_ia",
                "raison_non_participation_rdv",
                "autre_besoin_accompagnement",
                "autres_difficultes",
                "autres_freins",
                "autres_sources_de_progression",
                "autres_taches_realisees",
                "les_usages_inattendus",
                "mode_de_decouverte_usages",
                "autre_mode_de_decouverte",
                "autres_types_d_erreurs",
                "cas_usage_principal_teste",
                "commentaires",
                "deuxieme_cas_d_usage_teste",
                "commentaires2",
                "troisieme_cas_d_usage",
                "commentaires3",
                "autres_impacts_identifies",
                "autres_impacts_observes",
                "autres_raisons",
            ],
            ref_columns=[
                "id_niveau_d_usage_ia_post_expe_",
                "id_recommandation_collegues_mef",
                "id_sensation_montee_en_competences",
                "id_evolution_des_craintes_initiales",
                "id_utilite_metier_mef",
                "id_diminution_d_usage_ia_non_souveraines",
                "id_comparaison_autres_ia",
                "id_taux_moyen_de_correction_rep_assistant",
                "id_cu2_taux_moyen_de_correction_rep_assistant",
                "id_cu3_taux_moyen_de_correction_rep_assistant",
                "id_raisons_perte_de_temps",
                "id_ia_favorise_relations_humaines_",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_typologie_interaction = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_typologie_interaction",
            process.process_questionnaire_2_typologie_interaction,
            cols_to_keep=["no_id", "types_d_interactions_mef"],
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_formation_suivie = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_formation_suivie",
            process.process_questionnaire_2_formation_suivie,
            cols_mapping={
                "formation_ia_suivie_post_expe_": "id_formation_ia_suivie_post_expe_",
            },
            cols_to_keep=["no_id", "formation_ia_suivie_post_expe_"],
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_participation = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_participation",
            process.process_questionnaire_2_participation,
            cols_to_keep=["no_id", "participation_programme_rdv"],
            cols_mapping={
                "participation_programme_rdv": "id_participation_programme_rdv",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_freins = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_freins",
            process.process_questionnaire_2_freins,
            cols_to_keep=["no_id", "freins_a_l_utilisation"],
            cols_mapping={
                "freins_a_l_utilisation": "id_freins_a_l_utilisation",
            },
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_facteurs_progression = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_facteurs_progression",
            process.process_questionnaire_2_facteurs_progression,
            cols_to_keep=["no_id", "facteurs_de_progression"],
            cols_mapping={
                "facteurs_de_progression": "id_facteurs_de_progression",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_taches = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_taches",
            process.process_questionnaire_2_taches,
            cols_to_keep=["no_id", "taches_realisees_avec_ia"],
            cols_mapping={
                "taches_realisees_avec_ia": "id_taches_realisees_avec_ia",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_typologie_erreurs = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_typologie_erreurs",
            process.process_questionnaire_2_typologie_erreurs,
            cols_to_keep=["no_id", "types_d_erreurs_frequentes2"],
            cols_mapping={
                "types_d_erreurs_frequentes2": "id_types_d_erreurs_frequentes2",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_impact_observe = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_impact_observe",
            process.process_questionnaire_2_impact_observe,
            cols_to_keep=["no_id", "observations_des_impacts"],
            cols_mapping={
                "observations_des_impacts": "id_observations_des_impacts",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_impact_identifie = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_impact_identifie",
            process.process_questionnaire_2_impact_identifie,
            cols_to_keep=["no_id", "impacts_identifies_au_travail"],
            cols_mapping={
                "impacts_identifies_au_travail": "id_impacts_identifies_au_travail",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
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


@task_group()
def suivi_questionnaire_2_bis() -> None:
    questionnaire_2_bis = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_bis",
            process.process_questionnaire_2_bis,
            cols_to_keep=[
                "courriel",
                "avez_vous_deja_utilise_l_assistant_ia_",
                "autres_raisons",
                "ajouter_quelque_chose",
            ],
            txt_columns=["courriel", "autres_raisons", "ajouter_quelque_chose"],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_2_bis_raisons_non_utilisation = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_2_bis_raisons_non_utilisation",
            process.process_questionnaire_2_bis_raisons_non_utilisation,
            cols_to_keep=["courriel", "raisons_non_utilisation_assistant_ia"],
            cols_mapping={
                "raisons_non_utilisation_assistant_ia": "id_raisons_non_utilisation",
            },
            txt_columns=["courriel"],
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain(
        [
            questionnaire_2_bis(),
            questionnaire_2_bis_raisons_non_utilisation(),
        ]
    )


@task_group()
def suivi_questionnaire_3() -> None:
    questionnaire_3 = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3",
            process.process_questionnaire_3,
            cols_to_keep=[
                "no_id",
                "temps_fonction_exercee",
                "genre",
                "frequence_utilisation",
                "evolution_usage",
                "quelles_raisons_facons",
                "raisons_non_participation",
                "autres",
                "evaluation_niveau_acculturation",
                "evolution_sentiment",
                "autres_leviers",
                "impacts_taches_pro",
                "temps_gagnes",
                "impacts_taches_rebarbatives",
                "impact_perception",
                "sentiment_de_fierte",
                "experimentation_interne",
                "autres_outils",
                "satisfaction_autre_outil",
                "comparaison_autres_ia",
                "autres_fonctionnalites",
                "utilisation_moindre",
                "autres_risques",
                "recommandations",
                "etre_ambassadeur",
                "bonnes_pratiques",
                "autres_besoins_importants",
                "autres_besoins_moindres",
                "ameliorations",
                "aspects_a_ameliorer",
                "interface",
                "contenu",
                "connexions",
                "autre_retour_libre",
                "retours_libres",
            ],
            cols_mapping={
                "raisons_non_participation": "id_raisons_non_participation",
                "impacts_taches_pro": "id_impacts_taches_pro",
                "impacts_taches_rebarbatives": "id_impacts_taches_rebarbatives",
                "autres_outils": "id_autres_outils",
                "satisfaction_autre_outil": "id_satisfaction_autre_outil",
                "comparaison_autres_ia": "id_comparaison_autres_ia",
            },
            txt_columns=[
                "no_id",
                "quelles_raisons_facons",
                "autres",
                "autres_leviers",
                "autres_fonctionnalites",
                "autres_risques",
                "bonnes_pratiques",
                "autres_besoins_importants",
                "autres_besoins_moindres",
                "ameliorations",
                "aspects_a_ameliorer",
                "interface",
                "contenu",
                "connexions",
                "autre_retour_libre",
                "retours_libres",
            ],
            ref_columns=[
                "id_raisons_non_participation",
                "id_impacts_taches_pro",
                "id_impacts_taches_rebarbatives",
                "id_autres_outils",
                "id_satisfaction_autre_outil",
                "id_comparaison_autres_ia",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_3_formation_suivie = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3_formation_suivie",
            process.process_questionnaire_3_formation_suivie,
            cols_to_keep=["no_id", "formation_suivie"],
            cols_mapping={"formation_suivie": "id_formation_suivie"},
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_3_programme_rdv = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3_programme_rdv",
            process.process_questionnaire_3_programme_rdv,
            cols_to_keep=["no_id", "programme_de_rdv"],
            cols_mapping={"programme_de_rdv": "id_programme_de_rdv"},
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_3_leviers_progression = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3_leviers_progression",
            process.process_questionnaire_3_leviers_progression,
            cols_to_keep=["no_id", "leviers_progression"],
            cols_mapping={
                "leviers_progression": "id_leviers_progression",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_3_fonctionnalites = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3_fonctionnalites",
            process.process_questionnaire_3_fonctionnalites,
            cols_to_keep=["no_id", "fonctionnalites"],
            cols_mapping={"fonctionnalites": "id_fonctionnalites"},
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_3_risques_identifies = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3_risques_identifies",
            process.process_questionnaire_3_risques_identifies,
            cols_to_keep=["no_id", "risques_identifies"],
            cols_mapping={
                "risques_identifies": "id_risques_identifies",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_3_besoins_prioritaires = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3_besoins_prioritaires",
            process.process_questionnaire_3_besoins_prioritaires,
            cols_to_keep=["no_id", "besoins_prioritaires"],
            cols_mapping={
                "besoins_prioritaires": "id_besoins_prioritaires",
            },
            txt_columns=[
                "no_id",
            ],
        ),
        execution_options=config.execution_options,
    )
    questionnaire_3_besoins_moindres = create_task(
        pipeline=_grist_pipeline(
            "questionnaire_3_besoins_moindres",
            process.process_questionnaire_3_besoins_moindres,
            cols_to_keep=["no_id", "besoins_moindres"],
            cols_mapping={"besoins_moindres": "id_besoins_moindres"},
        ),
        execution_options=config.execution_options,
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
