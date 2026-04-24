import pandas as pd

from src.utils.process.structures import (
    convert_str_of_list_to_list,
    handle_grist_null_references,
)
from src.utils.process.text import normalize_whitespace_columns

"""
    Fonction de processing des référentiels du questionnaire 1
"""


def process_ref_q1_direction(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["direction"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


# def process_ref_q2_categorie_age(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#    return df


# def process_ref_q3_categorie_emploi(df: pd.DataFrame) -> pd.DataFrame:
#    logging.info(msg=NO_PROCESS_MSG)
#    return df


# def process_ref_q4_statut(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#   return df


def process_ref_q5_domaine(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["domaine"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q6_niveau_utilisation(df: pd.DataFrame) -> pd.DataFrame:
    col_to_keep = ["niveau_d_appropriation"]
    df = df.loc[:, col_to_keep]
    df = normalize_whitespace_columns(df=df, columns=col_to_keep)
    return df


# def process_ref_q8_attentes(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#    return df


def process_ref_q9_cas_usage(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["cas_d_usage"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


# def process_ref_q10_formation(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#   return df


# def process_ref_q11_besoins(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#    return df

"""
Processing des referenciels questionnaire 2
"""


def process_ref_q28_raisons_perte(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["raisons"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q25_impact_observe(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["observation"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q24_impact_identifie(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["impacts"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q23_taux_correction(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    col_to_keep = ["id", "taux_de_correction"]
    df = df.loc[:, col_to_keep]
    df = normalize_whitespace_columns(df=df, columns=["taux_de_correction"])
    return df


def process_ref_q22_erreurs(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["erreurs"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q20_autres_ia(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["comparaisons"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q16_taches(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["taches"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q14_evolution_craintes(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["evolutions"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q13_facteurs_progression(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["facteurs"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q10_principaux_freins(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["freins"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q6_participation_programme(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["participation"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q5_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["formation_suivie"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q3_niveau(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["niveau"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


def process_ref_q7_accords(df: pd.DataFrame) -> pd.DataFrame:
    txt_col = ["reponses"]
    df = normalize_whitespace_columns(df=df, columns=txt_col)
    return df


"""
Processing Entité
"""


def process_quota_entite(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    df = df.rename(
        columns={
            "nbre_de_connexion_effective_au_05_03_2026": "nbre_connexion_effective"
        }
    )
    cols_to_keep = [
        "id",
        "experimentation_demarree",
        "entite",
        "nbre_d_acces_previsionnels",
        "nb_acces_demande",
        "code",
        "nbre_connexion_effective",
        "nb_de_reponses_au_questionnaire",
        "nb_reponse_q2",
        "relance_dsci",
        "appel_a_candidature_dsci",
        "referent_ia",
        "courriel",
    ]
    df = df.loc[:, cols_to_keep]
    txt_cols = [
        "code",
        "relance_dsci",
        "appel_a_candidature_dsci",
        "referent_ia",
        "courriel",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    # Gestion des values
    col_values = ["nbre_d_acces_previsionnels", "nbre_connexion_effective"]
    for col in col_values:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df = df.drop_duplicates(subset="courriel", keep="last")
    return df


"""
Processing experimentateurs
"""


def process_experimentateurs(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = [
        "no_id",
        "entite",
        "nom_prenom",
        "parti",
        "service",
        "metier",
        "cas_d_usages",
        "courriel_corrige",
        "connecte_",
        "reponse_au_questionnaire_1",
        "reponse_au_questionnaire_2",
    ]
    df = df.loc[:, cols_to_keep]
    txt_cols = [
        "no_id",
        "entite",
        "nom_prenom",
        "parti",
        "service",
        "metier",
        "cas_d_usages",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    df = df.drop_duplicates(subset="courriel_corrige", keep="last")
    return df


"""
Processing Questionnaire 1 : profil des expérimentateurs

"""


def process_questionnaire_1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "direction": "id_direction",
            "domaine_professionnel": "id_domaine_professionnel",
            "niveau_d_utilisation_ia": "id_niveau_d_utilisation_ia",
        }
    )
    # Gestion des colonnes
    cols_to_keep = [
        "id",
        "mail_corrige",
        "id_direction",
        "tranche_age",
        "categorie_emploi",
        "statut",
        "id_domaine_professionnel",
        "metier",
        "situation_d_encadrement",
        "autres_experimentateurs",
        "id_niveau_d_utilisation_ia",
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
    ]
    df = df.loc[:, cols_to_keep]
    txt_cols = [
        "metier",
        "raisons_des_craintes",
        "attentes_experimentation",
        "autres_cas_usage_transverse",
        "cas_d_usage_metier",
        "autres_cas_usage_transverse",
        "autre_formation_suivie",
        "autre_besoin_accompagnement",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Gestion des references simples
    ref_cols = [
        "id_direction",
        "id_domaine_professionnel",
        "id_niveau_d_utilisation_ia",
    ]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = df.drop_duplicates(subset="mail_corrige", keep="last")
    return df


def process_questionnaire_1_cas_usage(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    df = df.rename(
        columns={
            "id": "id_questionnaire_1",
            "cas_d_usage_envisages": "id_cas_d_usage_envisages",
        }
    )
    cols_to_keep = ["id_questionnaire_1", "id_cas_d_usage_envisages"]
    df = df.loc[:, cols_to_keep]
    # Gestion des refs
    ref_cols = ["id_questionnaire_1", "id_cas_d_usage_envisages"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_cas_d_usage_envisages")
    df = df.explode(column="id_cas_d_usage_envisages")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_cas_d_usage_envisages"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_1", "id_cas_d_usage_envisages"]], index=False
    ) % (2**63)
    return df


def process_questionnaire_1_besoins_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "besoin_accompagnement"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(columns={"id": "id_questionnaire_1"})
    # Gestion des references
    ref_cols = ["id_questionnaire_1"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="besoin_accompagnement")
    df = df.explode(column="besoin_accompagnement")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["besoin_accompagnement"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_1", "besoin_accompagnement"]], index=False
    ) % (2**63)
    return df


"""
Processing Questionnaire 2 : Retour sur l'utiliation de l'assistant ia

"""


def process_questionnaire_2(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
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
        }
    )
    col_to_keep = [
        "id",
        "mail_corrige",
        "direction",
        # "types_d_interactions_mef",
        "autres_types_d_interactions",
        "id_niveau_d_usage_ia_post_expe_",
        "frequence_d_usage_assistant_ia",
        "autres_formation_ia",
        "raison_non_participation_rdv",
        "autre_besoin_accompagnement",
        "apprentissage_assistant_ia_ressenti_",
        "difficultes_techniques_rencontrees2",
        "autres_difficultes",
        "autres_taches_realisees",
        "autres_freins",
        "id_recommandation_collegues_mef",
        "id_sensation_montee_en_competences",
        "autres_sources_de_progression",
        "id_evolution_des_craintes_initiales",
        "id_utilite_metier_mef",
        "decouverte_d_usages_inattendus",
        "les_usages_inattendus",
        "mode_de_decouverte_usages",
        "autre_mode_de_decouverte",
        "id_diminution_d_usage_ia_non_souveraines",
        "id_comparaison_autres_ia",
        "frequence_des_erreurs",
        "autres_types_d_erreurs",
        "cas_usage_principal_teste",
        "temps_economise_par_semaine",
        "cu1_nombre_echanges_moyens_affinage_reponse",
        "id_taux_moyen_de_correction_rep_assistant",
        "pertinence_assistant_ia",
        "commentaires",
        "deuxieme_cas_d_usage_teste",
        "cu2_temps_economise_par_semaine",
        "cu2_nombre_echanges_moyens",
        "id_cu2_taux_moyen_de_correction_rep_assistant",
        "cu2_pertinence_assistant_ia",
        "commentaires2",
        "troisieme_cas_d_usage",
        "cu3_temps_economise_par_semaine",
        "cu3_nombre_echanges_moyens_affinage_reponse",
        "id_cu3_taux_moyen_de_correction_rep_assistant",
        "cu3_pertinence_assistant_ia",
        "commentaires3",
        "autres_impacts_identifies",
        "autres_impacts_observes",
        "impact_sur_le_temps_de_travail",
        "estimation_globale_gain_de_temps",
        "id_raisons_perte_de_temps",
        "autres_raisons",
        "id_ia_favorise_relations_humaines_",
    ]
    txt_cols = [
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
    ]
    df = df.loc[:, col_to_keep]
    # Gestion des references simples
    ref_cols = [
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
    ]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = df.drop_duplicates(subset="mail_corrige", keep="last")
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


def process_questionnaire2_typologie_interaction(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "types_d_interactions_mef"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(columns={"id": "id_questionnaire_2"})
    # Gestion des references
    ref_cols = ["id_questionnaire_2"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="types_d_interactions_mef")
    df = df.explode(column="types_d_interactions_mef")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["types_d_interactions_mef"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "types_d_interactions_mef"]], index=False
    ) % (2**63)
    return df


def process_questionnaire2_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "formation_ia_suivie_post_expe_"]
    df = df.loc[:, cols_to_keep]

    # Renommage
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "formation_ia_suivie_post_expe_": "id_formation_ia_suivie_post_expe_",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_formation_ia_suivie_post_expe_"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(
        df=df, col_to_convert="id_formation_ia_suivie_post_expe_"
    )
    df = df.explode(column="id_formation_ia_suivie_post_expe_")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_formation_ia_suivie_post_expe_"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_formation_ia_suivie_post_expe_"]], index=False
    ) % (2**63)
    return df


def process_questionnaire2_participation(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "participation_programme_rdv"]
    df = df.loc[:, cols_to_keep]

    # Renommage
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "participation_programme_rdv": "id_participation_programme_rdv",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_participation_programme_rdv"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(
        df=df, col_to_convert="id_participation_programme_rdv"
    )
    df = df.explode(column="id_participation_programme_rdv")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_participation_programme_rdv"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_participation_programme_rdv"]], index=False
    ) % (2**63)
    return df


def process_questionnaire2_freins(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "freins_a_l_utilisation"]
    df = df.loc[:, cols_to_keep]

    # Renommage
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "freins_a_l_utilisation": "id_freins_a_l_utilisation",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_freins_a_l_utilisation"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_freins_a_l_utilisation")
    df = df.explode(column="id_freins_a_l_utilisation")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_freins_a_l_utilisation"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_freins_a_l_utilisation"]], index=False
    ) % (2**63)
    return df


def process_questionnaire2_facteurs_progression(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "facteurs_de_progression"]
    df = df.loc[:, cols_to_keep]

    # Renommage
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "facteurs_de_progression": "id_facteurs_de_progression",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_facteurs_de_progression"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_facteurs_de_progression")
    df = df.explode(column="id_facteurs_de_progression")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_facteurs_de_progression"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_facteurs_de_progression"]], index=False
    ) % (2**63)
    return df

    return df


def process_questionnaire2_taches(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "taches_realisees_avec_ia"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "taches_realisees_avec_ia": "id_taches_realisees_avec_ia",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_taches_realisees_avec_ia"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(
        df=df, col_to_convert="id_taches_realisees_avec_ia"
    )
    df = df.explode(column="id_taches_realisees_avec_ia")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_taches_realisees_avec_ia"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_taches_realisees_avec_ia"]], index=False
    ) % (2**63)
    return df


def process_questionnaire2_typologie_erreurs(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "types_d_erreurs_frequentes2"]
    df = df.loc[:, cols_to_keep]
    # Renommage
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "types_d_erreurs_frequentes2": "id_types_d_erreurs_frequentes2",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_types_d_erreurs_frequentes2"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(
        df=df, col_to_convert="id_types_d_erreurs_frequentes2"
    )
    df = df.explode(column="id_types_d_erreurs_frequentes2")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_types_d_erreurs_frequentes2"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_types_d_erreurs_frequentes2"]], index=False
    ) % (2**63)

    return df


def process_questionnaire2_observation_impact(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "observations_des_impacts"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "observations_des_impacts": "id_observations_des_impacts",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_observations_des_impacts"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(
        df=df, col_to_convert="id_observations_des_impacts"
    )
    df = df.explode(column="id_observations_des_impacts")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_observations_des_impacts"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_observations_des_impacts"]], index=False
    ) % (2**63)
    return df


def process_questionnaire2_impact_identifie(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "impacts_identifies_au_travail"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={
            "id": "id_questionnaire_2",
            "impacts_identifies_au_travail": "id_impacts_identifies_au_travail",
        }
    )
    # Gestion des refs
    ref_cols = ["id_questionnaire_2", "id_impacts_identifies_au_travail"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_impacts_identifies_au_travail")
    df = df.explode(column="id_impacts_identifies_au_travail")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_impacts_identifies_au_travail"])
    # Nouvel ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_questionnaire_2", "id_impacts_identifies_au_travail"]], index=False
    ) % (2**63)
    return df
