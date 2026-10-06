import pandas as pd
from modules.constants import METADATA_COLS
from modules.generic_processing.structures import (
    convert_str_of_list_to_list,
)
from modules.infra.airflow.common_tasks.grist import generic_grist_processing


# =============================================================
# Fonction de processing des référentiels communs à tous les questionnaires
# =============================================================
def process_ref_niveau_appropriation(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "niveau_d_appropriation", "niveau_appropriation_libelle_court"],
        txt_columns=["niveau_d_appropriation", "niveau_appropriation_libelle_court"],
    )
    return df


def process_ref_niveau_accord(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "niveau_accord"],
        txt_columns=["niveau_accord"],
    )
    return df


def process_ref_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "formation_suivie"],
        txt_columns=["formation_suivie"],
    )
    return df


def process_ref_participation_programme(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "participation"],
        txt_columns=["participation"],
    )
    return df


# =============================================================
# Fonction de processing des référentiels du questionnaire 1
# =============================================================
def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "direction"],
        txt_columns=["direction"],
    )
    return df


def process_ref_domaine_professionnel(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "domaine"],
        txt_columns=["domaine"],
    )
    return df


def process_ref_cas_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "cas_d_usage"],
        txt_columns=["cas_d_usage"],
    )
    return df


# =============================================================
# Processing des referenciels questionnaire 2
# =============================================================
def process_ref_raison_perte_temps(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "raisons"],
        txt_columns=["raisons"],
    )
    return df


def process_ref_impact_observation(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "observation"],
        txt_columns=["observation"],
    )
    return df


def process_ref_impact_identifie(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "impact"],
        txt_columns=["impact"],
    )
    return df


def process_ref_taux_correction(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "taux_de_correction"],
        txt_columns=["taux_de_correction"],
    )
    return df


def process_ref_type_erreur_ia(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "type_erreur"],
        txt_columns=["type_erreur"],
    )
    return df


def process_ref_tache_realise(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "taches"],
        txt_columns=["taches"],
    )
    return df


def process_ref_facteur_progression(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "facteurs"],
        txt_columns=["facteurs"],
    )
    return df


def process_ref_evolution_crainte(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "evolutions"],
        txt_columns=["evolutions"],
    )
    return df


def process_ref_comparaison_autres_ia(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "comparaisons"],
        txt_columns=["comparaisons"],
    )
    return df


def process_ref_frein_utilisation(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "freins"],
        txt_columns=["freins"],
    )
    return df


def process_ref_raison_non_utilisation(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "raisons"],
        txt_columns=["raisons"],
    )
    return df


# =============================================================
# Processing référentiel questionnaire 3 : Usage et ressentis face à l'Assistant IA
# =============================================================
def process_ref_raison_non_participation(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "raisons"],
        txt_columns=["raisons"],
    )
    return df


def process_ref_levier_progression(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "leviers"],
        txt_columns=["leviers"],
    )
    return df


def process_ref_impact_tache_pro(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "impacts"],
        txt_columns=["impacts"],
    )
    return df


def process_ref_impact_tache_rebarbative(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "taches_rebarbatives"],
        txt_columns=["taches_rebarbatives"],
    )
    return df


def process_ref_autre_outil_ia(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "autres_outils"],
        txt_columns=["autres_outils"],
    )
    return df


def process_ref_comparaison_autre_ia(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "comparaisons"],
        txt_columns=["comparaisons"],
    )
    return df


def process_ref_autre_fonctionnalite(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "fonctionnalites"],
        txt_columns=["fonctionnalites"],
    )
    return df


def process_ref_risque(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "risques"],
        txt_columns=["risques"],
    )
    return df


def process_ref_besoin(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["id", "besoins"],
        txt_columns=["besoins"],
    )
    return df


# =============================================================
# Processing Entité
# =============================================================
def process_quota_par_entite(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
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
    )

    df = df.drop_duplicates(subset="courriel", keep="last")
    return df


# =============================================================
# Processing experimentateurs
# =============================================================
def process_experimentateurs(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
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
    )
    df = df.dropna(subset=["courriel"])
    df = df.drop_duplicates(subset="courriel", keep="last")
    return df


# =============================================================
# Processing Questionnaire 1 : profil des expérimentateurs
# =============================================================
def process_q1(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
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
    )

    df = df.drop_duplicates(subset="no_id", keep="last")
    return df


def process_q1_cas_usage(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "cas_d_usage_envisages"],
        cols_mapping={
            "cas_d_usage_envisages": "id_cas_d_usage_envisages",
        },
        txt_columns=[
            "no_id",
        ],
    )
    # Convertion, Explode et dropna
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_cas_d_usage_envisages")
    df = df.explode(column="id_cas_d_usage_envisages")
    df = df.dropna(subset=["id_cas_d_usage_envisages"])

    return df


def process_q1_besoins_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "besoin_accompagnement"],
        txt_columns=[
            "no_id",
        ],
    )

    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="besoin_accompagnement")
    df = df.explode(column="besoin_accompagnement")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["besoin_accompagnement"])

    return df


# =============================================================
# Processing Questionnaire 2 : Retour sur l'utiliation de l'assistant ia
# =============================================================
def process_q2(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
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
    )

    df = df.dropna(subset=["no_id"])
    df = df.drop_duplicates(subset="no_id", keep="last")
    return df


def process_q2_typologie_interaction(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "types_d_interactions_mef"],
        txt_columns=[
            "no_id",
        ],
    )
    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="types_d_interactions_mef")
    df = df.explode(column="types_d_interactions_mef")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["types_d_interactions_mef"])

    return df


def process_q2_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_mapping={
            "formation_ia_suivie_post_expe_": "id_formation_ia_suivie_post_expe_",
        },
        cols_to_keep=["no_id", "formation_ia_suivie_post_expe_"],
        txt_columns=[
            "no_id",
        ],
    )
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_formation_ia_suivie_post_expe_")
    df = df.explode(column="id_formation_ia_suivie_post_expe_")
    df = df.dropna(subset=["id_formation_ia_suivie_post_expe_"])
    df = df.drop_duplicates()

    return df


def process_q2_participation(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "participation_programme_rdv"],
        cols_mapping={
            "participation_programme_rdv": "id_participation_programme_rdv",
        },
        txt_columns=[
            "no_id",
        ],
    )
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_participation_programme_rdv")
    df = df.explode(column="id_participation_programme_rdv")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_participation_programme_rdv"])
    df = df.drop_duplicates()

    return df


def process_q2_freins(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "freins_a_l_utilisation"],
        cols_mapping={
            "freins_a_l_utilisation": "id_freins_a_l_utilisation",
        },
    )

    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_freins_a_l_utilisation")
    df = df.explode(column="id_freins_a_l_utilisation")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_freins_a_l_utilisation"])

    return df


def process_q2_facteurs_progression(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "facteurs_de_progression"],
        cols_mapping={
            "facteurs_de_progression": "id_facteurs_de_progression",
        },
        txt_columns=[
            "no_id",
        ],
    )

    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_facteurs_de_progression")
    df = df.explode(column="id_facteurs_de_progression")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_facteurs_de_progression"])

    return df


def process_q2_taches(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "taches_realisees_avec_ia"],
        cols_mapping={
            "taches_realisees_avec_ia": "id_taches_realisees_avec_ia",
        },
        txt_columns=[
            "no_id",
        ],
    )

    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_taches_realisees_avec_ia")
    df = df.explode(column="id_taches_realisees_avec_ia")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_taches_realisees_avec_ia"])

    return df


def process_q2_typologie_erreurs(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "types_d_erreurs_frequentes2"],
        cols_mapping={
            "types_d_erreurs_frequentes2": "id_types_d_erreurs_frequentes2",
        },
        txt_columns=[
            "no_id",
        ],
    )

    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_types_d_erreurs_frequentes2")
    df = df.explode(column="id_types_d_erreurs_frequentes2")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_types_d_erreurs_frequentes2"])

    return df


def process_q2_impact_observe(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "observations_des_impacts"],
        cols_mapping={
            "observations_des_impacts": "id_observations_des_impacts",
        },
        txt_columns=[
            "no_id",
        ],
    )

    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_observations_des_impacts")
    df = df.explode(column="id_observations_des_impacts")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_observations_des_impacts"])

    return df


def process_q2_impact_identifie(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "impacts_identifies_au_travail"],
        cols_mapping={
            "impacts_identifies_au_travail": "id_impacts_identifies_au_travail",
        },
        txt_columns=[
            "no_id",
        ],
    )

    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_impacts_identifies_au_travail")
    df = df.explode(column="id_impacts_identifies_au_travail")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_impacts_identifies_au_travail"])

    return df


# =============================================================
# Processing du questionnaire2_bis
# =============================================================
def process_q2bis(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=[
            "courriel",
            "avez_vous_deja_utilise_l_assistant_ia_",
            "autres_raisons",
            "ajouter_quelque_chose",
        ],
        txt_columns=["courriel", "autres_raisons", "ajouter_quelque_chose"],
    )
    return df


def process_q2bis_raisons_non_utilisation(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["courriel", "raisons_non_utilisation_assistant_ia"],
        cols_mapping={
            "raisons_non_utilisation_assistant_ia": "id_raisons_non_utilisation",
        },
        txt_columns=["courriel"],
    )

    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_raisons_non_utilisation")
    df = df.explode(column="id_raisons_non_utilisation")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_raisons_non_utilisation"])

    return df


# =============================================================
# Processing du questionnaire 3
# =============================================================
def process_q3(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
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
            "comparaison_autres_ia": "id_comparaison_autres_ia",
            "utilisation_moindre": "id_utilisation_moindre",
            "recommandations": "id_recommandations",
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
            "autres_outils",
            "satisfaction_autre_outil",
        ],
        ref_columns=[
            "id_raisons_non_participation",
            "id_impacts_taches_pro",
            "id_impacts_taches_rebarbatives",
            "id_comparaison_autres_ia",
            "id_utilisation_moindre",
            "id_recommandations",
        ],
    )
    df = df.drop_duplicates(subset="no_id", keep="last")
    return df


def process_q3_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "formation_suivie"],
        cols_mapping={"formation_suivie": "id_formation_suivie"},
        txt_columns=[
            "no_id",
        ],
    )
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_formation_suivie")
    df = df.explode(column="id_formation_suivie")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_formation_suivie"])

    return df


def process_q3_programme_rdv(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "programme_de_rdv"],
        cols_mapping={"programme_de_rdv": "id_programme_de_rdv"},
        txt_columns=[
            "no_id",
        ],
    )
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_programme_de_rdv")
    df = df.explode(column="id_programme_de_rdv")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_programme_de_rdv"])

    return df


def process_q3_leviers_progression(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "leviers_progression"],
        cols_mapping={
            "leviers_progression": "id_leviers_progression",
        },
        txt_columns=[
            "no_id",
        ],
    )
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_leviers_progression")
    df = df.explode(column="id_leviers_progression")
    # Nettoyage
    df = df.dropna(subset=["id_leviers_progression"])

    return df


def process_q3_fonctionnalites(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "fonctionnalites"],
        cols_mapping={"fonctionnalites": "id_fonctionnalites"},
        txt_columns=[
            "no_id",
        ],
    )
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_fonctionnalites")
    df = df.explode(column="id_fonctionnalites")
    # Nettoyage
    df = df.dropna(subset=["id_fonctionnalites"])

    return df


def process_q3_risques_identifies(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "risques_identifies"],
        cols_mapping={
            "risques_identifies": "id_risques_identifies",
        },
        txt_columns=[
            "no_id",
        ],
    )
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_risques_identifies")
    df = df.explode(column="id_risques_identifies")
    # Nettoyage
    df = df.dropna(subset=["id_risques_identifies"])

    return df


def process_q3_besoins_prioritaires(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "besoins_prioritaires"],
        cols_mapping={
            "besoins_prioritaires": "id_besoins_prioritaires",
        },
        txt_columns=[
            "no_id",
        ],
    )
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_besoins_prioritaires")
    df = df.explode(column="id_besoins_prioritaires")
    # Nettoyage
    df = df.dropna(subset=["id_besoins_prioritaires"])

    return df


def process_q3_besoins_moindres(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_to_keep=["no_id", "besoins_moindres"],
        cols_mapping={"besoins_moindres": "id_besoins_moindres"},
    )
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_besoins_moindres")
    df = df.explode(column="id_besoins_moindres")
    # Nettoyage
    df = df.dropna(subset=["id_besoins_moindres"])

    return df


# =============================================================
# Fonction de processing des référentiels communs à tous les questionnaires
# =============================================================
def _left_merge_ref(
    df_left: pd.DataFrame,
    left_key: str,
    df_ref: pd.DataFrame,
    ref_cols: list[str],
) -> pd.DataFrame:
    """Merge selected reference columns without leaking duplicate right-side id columns."""
    ref_lookup = df_ref.loc[:, ["id", *ref_cols]].rename(columns={"id": "__ref_id__"})
    return df_left.merge(right=ref_lookup, how="left", left_on=left_key, right_on="__ref_id__").drop(
        columns=["__ref_id__"]
    )


def process_dim_experimentateurs(
    df_experimentateurs: pd.DataFrame,
    df_q1: pd.DataFrame,
    df_q3: pd.DataFrame,
    df_ref_direction: pd.DataFrame,
    df_ref_domaine_professionnel: pd.DataFrame,
    df_ref_niveau_appropriation: pd.DataFrame,
) -> pd.DataFrame:
    df_expe_clean = df_experimentateurs.drop(columns=METADATA_COLS)
    df_q1 = df_q1.loc[
        :,
        [
            "no_id",
            "id_direction",
            "tranche_age",
            "categorie_emploi",
            "statut",
            "id_domaine_professionnel",
            "id_niveau_d_utilisation_ia",
            "situation_d_encadrement",
            "usage_ia_perso_avant_expe",
            "usage_ia_pro_avant_expe",
            "craintes_usage_ia_pro",
        ],
    ]
    df_q3 = df_q3.loc[:, ["no_id", "temps_fonction_exercee", "genre", "frequence_utilisation", "evolution_usage"]]
    df_ref_direction = df_ref_direction.drop(columns=METADATA_COLS)
    df_ref_domaine_professionnel = df_ref_domaine_professionnel.drop(columns=METADATA_COLS)
    df_ref_niveau_appropriation = df_ref_niveau_appropriation.drop(columns=METADATA_COLS)

    df = df_expe_clean.merge(right=df_q1, how="left", left_on="no_id", right_on="no_id").merge(
        right=df_q3, how="left", left_on="no_id", right_on="no_id"
    )
    df = _left_merge_ref(df, "id_direction", df_ref_direction, ["direction"])
    df = _left_merge_ref(df, "id_domaine_professionnel", df_ref_domaine_professionnel, ["domaine"])
    df = _left_merge_ref(
        df,
        "id_niveau_d_utilisation_ia",
        df_ref_niveau_appropriation,
        ["niveau_appropriation_libelle_court"],
    )
    df = df.rename(
        columns={
            "domaine": "domaine_professionnel",
            "niveau_appropriation_libelle_court": "niveau_d_utilisation_ia",
        }
    )
    return df


def process_dim_q1(
    df_q1: pd.DataFrame,
    df_ref_direction: pd.DataFrame,
    df_ref_domaine_professionnel: pd.DataFrame,
    df_ref_niveau_appropriation: pd.DataFrame,
) -> pd.DataFrame:
    df_q1 = df_q1.drop(columns=METADATA_COLS)
    df_ref_direction = df_ref_direction.drop(columns=METADATA_COLS)
    df_ref_domaine_professionnel = df_ref_domaine_professionnel.drop(columns=METADATA_COLS)
    df_ref_niveau_appropriation = df_ref_niveau_appropriation.drop(columns=METADATA_COLS)

    df = _left_merge_ref(df_q1, "id_direction", df_ref_direction, ["direction"])
    df = _left_merge_ref(df, "id_domaine_professionnel", df_ref_domaine_professionnel, ["domaine"])
    df = df.rename(columns={"domaine": "domaine_professionnel"})
    df = _left_merge_ref(
        df,
        "id_niveau_d_utilisation_ia",
        df_ref_niveau_appropriation,
        ["niveau_appropriation_libelle_court"],
    )
    df = df.rename(columns={"niveau_appropriation_libelle_court": "niveau_d_utilisation_ia"})
    return df


def process_dim_q2(
    df_q2: pd.DataFrame,
    df_ref_niveau_appropriation: pd.DataFrame,
    df_ref_niveau_accord: pd.DataFrame,
    df_ref_evolution_crainte: pd.DataFrame,
    df_ref_comparaison_autre_ia: pd.DataFrame,
    df_ref_taux_correction: pd.DataFrame,
    df_ref_raison_perte_temps: pd.DataFrame,
) -> pd.DataFrame:
    df_q2 = df_q2.drop(columns=METADATA_COLS)
    df_ref_niveau_appropriation = df_ref_niveau_appropriation.drop(columns=METADATA_COLS)
    df_ref_niveau_accord = df_ref_niveau_accord.drop(columns=METADATA_COLS)
    df_ref_evolution_crainte = df_ref_evolution_crainte.drop(columns=METADATA_COLS)
    df_ref_comparaison_autre_ia = df_ref_comparaison_autre_ia.drop(columns=METADATA_COLS)
    df_ref_taux_correction = df_ref_taux_correction.drop(columns=METADATA_COLS)
    df_ref_raison_perte_temps = df_ref_raison_perte_temps.drop(columns=METADATA_COLS)

    df = _left_merge_ref(
        df_q2,
        "id_niveau_d_usage_ia_post_expe_",
        df_ref_niveau_appropriation,
        ["niveau_appropriation_libelle_court"],
    )
    df = df.rename(columns={"niveau_appropriation_libelle_court": "niveau_d_usage_ia"})
    df = _left_merge_ref(df, "id_recommandation_collegues_mef", df_ref_niveau_accord, ["niveau_accord"])
    df = df.rename(columns={"niveau_accord": "recommandation_collegues_mef"})
    df = _left_merge_ref(df, "id_sensation_montee_en_competences", df_ref_niveau_accord, ["niveau_accord"])
    df = df.rename(columns={"niveau_accord": "sensation_montee_en_competences"})
    df = _left_merge_ref(df, "id_evolution_des_craintes_initiales", df_ref_evolution_crainte, ["evolutions"])
    df = df.rename(columns={"evolutions": "evolution_des_craintes_initiales"})
    df = _left_merge_ref(df, "id_utilite_metier_mef", df_ref_niveau_accord, ["niveau_accord"])
    df = df.rename(columns={"niveau_accord": "utilite_metier_mef"})
    df = _left_merge_ref(df, "id_diminution_d_usage_ia_non_souveraines", df_ref_niveau_accord, ["niveau_accord"])
    df = df.rename(columns={"niveau_accord": "diminution_d_usage_ia_non_souveraines"})
    df = _left_merge_ref(df, "id_comparaison_autres_ia", df_ref_comparaison_autre_ia, ["comparaisons"])
    df = df.rename(columns={"comparaisons": "comparaison_autre_ia"})
    df = _left_merge_ref(
        df,
        "id_taux_moyen_de_correction_rep_assistant",
        df_ref_taux_correction,
        ["taux_de_correction"],
    )
    df = df.rename(columns={"taux_de_correction": "cu1_taux_moyen_de_correction_rep_assistant"})
    df = _left_merge_ref(
        df,
        "id_cu2_taux_moyen_de_correction_rep_assistant",
        df_ref_taux_correction,
        ["taux_de_correction"],
    )
    df = df.rename(columns={"taux_de_correction": "cu2_taux_moyen_de_correction_rep_assistant"})
    df = _left_merge_ref(
        df,
        "id_cu3_taux_moyen_de_correction_rep_assistant",
        df_ref_taux_correction,
        ["taux_de_correction"],
    )
    df = df.rename(columns={"taux_de_correction": "cu3_taux_moyen_de_correction_rep_assistant"})
    df = _left_merge_ref(df, "id_raisons_perte_de_temps", df_ref_raison_perte_temps, ["raisons"])
    df = df.rename(columns={"raisons": "raisons_perte_de_temps"})
    df = _left_merge_ref(df, "id_ia_favorise_relations_humaines_", df_ref_niveau_accord, ["niveau_accord"])
    df = df.rename(columns={"niveau_accord": "ia_favorise_relations_humaines"})
    return df


def process_dim_q2_duckdb_prototype(
    df_q2: pd.DataFrame,
    df_ref_niveau_appropriation: pd.DataFrame,
    df_ref_niveau_accord: pd.DataFrame,
    df_ref_evolution_crainte: pd.DataFrame,
    df_ref_comparaison_autre_ia: pd.DataFrame,
    df_ref_taux_correction: pd.DataFrame,
    df_ref_raison_perte_temps: pd.DataFrame,
) -> pd.DataFrame:
    """Prototype DuckDB version of process_dim_q2 using explicit SQL joins."""
    try:
        import duckdb
    except ImportError as exc:
        raise ImportError("duckdb is required for process_dim_q2_duckdb_prototype") from exc

    df_q2_clean = df_q2.drop(columns=METADATA_COLS)
    ref_niveau_appropriation = df_ref_niveau_appropriation.drop(columns=METADATA_COLS)
    ref_niveau_accord = df_ref_niveau_accord.drop(columns=METADATA_COLS)
    ref_evolution_crainte = df_ref_evolution_crainte.drop(columns=METADATA_COLS)
    ref_comparaison_autre_ia = df_ref_comparaison_autre_ia.drop(columns=METADATA_COLS)
    ref_taux_correction = df_ref_taux_correction.drop(columns=METADATA_COLS)
    ref_raison_perte_temps = df_ref_raison_perte_temps.drop(columns=METADATA_COLS)

    con = duckdb.connect(database=":memory:")
    try:
        con.register("q2", df_q2_clean)
        con.register("ref_niveau_appropriation", ref_niveau_appropriation)
        con.register("ref_niveau_accord", ref_niveau_accord)
        con.register("ref_evolution_crainte", ref_evolution_crainte)
        con.register("ref_comparaison_autre_ia", ref_comparaison_autre_ia)
        con.register("ref_taux_correction", ref_taux_correction)
        con.register("ref_raison_perte_temps", ref_raison_perte_temps)

        # Explicit aliases keep output columns deterministic and avoid suffix collisions.
        query = """
            SELECT
                q2.*,
                n_app.niveau_appropriation_libelle_court AS niveau_d_usage_ia,
                acc_rec.niveau_accord AS recommandation_collegues_mef,
                acc_sens.niveau_accord AS sensation_montee_en_competences,
                evo.evolutions AS evolution_des_craintes_initiales,
                acc_util.niveau_accord AS utilite_metier_mef,
                acc_dim.niveau_accord AS diminution_d_usage_ia_non_souveraines,
                comp.comparaisons AS comparaison_autre_ia,
                tx1.taux_de_correction AS cu1_taux_moyen_de_correction_rep_assistant,
                tx2.taux_de_correction AS cu2_taux_moyen_de_correction_rep_assistant,
                tx3.taux_de_correction AS cu3_taux_moyen_de_correction_rep_assistant,
                rtp.raisons AS raisons_perte_de_temps,
                acc_hum.niveau_accord AS ia_favorise_relations_humaines
            FROM q2
            LEFT JOIN ref_niveau_appropriation n_app
                ON q2.id_niveau_d_usage_ia_post_expe_ = n_app.id
            LEFT JOIN ref_niveau_accord acc_rec
                ON q2.id_recommandation_collegues_mef = acc_rec.id
            LEFT JOIN ref_niveau_accord acc_sens
                ON q2.id_sensation_montee_en_competences = acc_sens.id
            LEFT JOIN ref_evolution_crainte evo
                ON q2.id_evolution_des_craintes_initiales = evo.id
            LEFT JOIN ref_niveau_accord acc_util
                ON q2.id_utilite_metier_mef = acc_util.id
            LEFT JOIN ref_niveau_accord acc_dim
                ON q2.id_diminution_d_usage_ia_non_souveraines = acc_dim.id
            LEFT JOIN ref_comparaison_autre_ia comp
                ON q2.id_comparaison_autres_ia = comp.id
            LEFT JOIN ref_taux_correction tx1
                ON q2.id_taux_moyen_de_correction_rep_assistant = tx1.id
            LEFT JOIN ref_taux_correction tx2
                ON q2.id_cu2_taux_moyen_de_correction_rep_assistant = tx2.id
            LEFT JOIN ref_taux_correction tx3
                ON q2.id_cu3_taux_moyen_de_correction_rep_assistant = tx3.id
            LEFT JOIN ref_raison_perte_temps rtp
                ON q2.id_raisons_perte_de_temps = rtp.id
            LEFT JOIN ref_niveau_accord acc_hum
                ON q2.id_ia_favorise_relations_humaines_ = acc_hum.id
        """
        return con.execute(query).df()
    finally:
        con.close()


def process_dim_q3(
    df_q3: pd.DataFrame,
    df_ref_raison_non_participation: pd.DataFrame,
    df_ref_impacts_taches_pro: pd.DataFrame,
    df_ref_impacts_taches_rebarbatives: pd.DataFrame,
    df_ref_comparaison_autre_ia: pd.DataFrame,
    df_ref_niveau_accord: pd.DataFrame,
) -> pd.DataFrame:
    df_q3 = df_q3.drop(columns=METADATA_COLS)
    df_ref_raison_non_participation = df_ref_raison_non_participation.drop(columns=METADATA_COLS)
    df_ref_impacts_taches_pro = df_ref_impacts_taches_pro.drop(columns=METADATA_COLS)
    df_ref_impacts_taches_rebarbatives = df_ref_impacts_taches_rebarbatives.drop(columns=METADATA_COLS)
    df_ref_niveau_accord = df_ref_niveau_accord.drop(columns=METADATA_COLS)
    df_ref_comparaison_autre_ia = df_ref_comparaison_autre_ia.drop(columns=METADATA_COLS)

    df = _left_merge_ref(
        df_q3,
        "id_raisons_non_participation",
        df_ref_raison_non_participation,
        ["raisons"],
    )
    df = df.rename(columns={"raisons": "raisons_non_participation"})
    df = _left_merge_ref(df, "id_impacts_taches_pro", df_ref_impacts_taches_pro, ["impacts"])
    df = df.rename(columns={"impacts": "impacts_taches_pro"})
    df = _left_merge_ref(
        df,
        "id_impacts_taches_rebarbatives",
        df_ref_impacts_taches_rebarbatives,
        ["taches_rebarbatives"],
    )
    df = df.rename(columns={"taches_rebarbatives": "impacts_taches_rebarbatives"})
    df = _left_merge_ref(df, "id_comparaison_autres_ia", df_ref_comparaison_autre_ia, ["comparaisons"])
    df = df.rename(columns={"comparaisons": "comparaisons_autres_ia"})
    df = _left_merge_ref(df, "id_utilisation_moindre", df_ref_niveau_accord, ["niveau_accord"])
    df = df.rename(columns={"niveau_accord": "utilisation_moindre"})
    df = _left_merge_ref(df, "id_recommandations", df_ref_niveau_accord, ["niveau_accord"])
    df = df.rename(columns={"niveau_accord": "recommandations"})
    return df
