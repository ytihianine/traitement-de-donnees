import logging

import pandas as pd
from modules.constants import NO_PROCESS_MSG
from modules.utils.process.structures import (
    convert_str_of_list_to_list,
)


# =============================================================
# Fonction de processing des référentiels du questionnaire 1
# =============================================================
def process_ref_q1_direction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
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
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q6_niveau_utilisation(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# def process_ref_q8_attentes(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#    return df


def process_ref_q9_cas_usage(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# def process_ref_q10_formation(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#   return df


# def process_ref_q11_besoins(df: pd.DataFrame) -> pd.DataFrame:
#   logging.info(msg=NO_PROCESS_MSG)
#    return df


# =============================================================
# Processing des referenciels questionnaire 2
# =============================================================
def process_ref_q28_raisons_perte(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q25_impact_observe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q24_impact_identifie(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q23_taux_correction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q22_typologie_erreurs(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q20_autres_ia(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q16_taches(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q14_evolution_craintes(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q13_facteurs_progression(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q10_principaux_freins(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q6_participation_programme(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q5_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q3_niveau(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q7_accords(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# =============================================================
# Processing référentiel questionnaire2_bis : Jamais connecté à l'Assistant IA
# =============================================================
def process_ref_raisons_non_utilisation(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# =============================================================
# Processing référentiel questionnaire 3 : Usage et ressentis face à l'Assistant IA
# =============================================================


def process_ref_q6_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q7_particip_programme(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q8_raisons_non_participation(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q11_leviers_progressions(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q12_impacts_taches_pro(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q14_taches_rebarbativ(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q17_autres_outils(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q17_satisfaction_autre_outil(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q18_comparaisons(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q19_fonctionnalites(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q21_risques_identifies(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_q25_besoins(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# =============================================================
# Processing Entité
# =============================================================
def process_quota_par_entite(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="courriel", keep="last")
    return df


# =============================================================
# Processing experimentateurs
# =============================================================
def process_experimentateurs(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["courriel"])
    df = df.drop_duplicates(subset="courriel", keep="last")
    return df


# =============================================================
# Processing Questionnaire 1 : profil des expérimentateurs
# =============================================================
def process_questionnaire_1(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="no_id", keep="last")
    return df


def process_questionnaire_1_cas_usage(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion, Explode et dropna
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_cas_d_usage_envisages")
    df = df.explode(column="id_cas_d_usage_envisages")
    df = df.dropna(subset=["id_cas_d_usage_envisages"])

    return df


def process_questionnaire_1_besoins_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="besoin_accompagnement")
    df = df.explode(column="besoin_accompagnement")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["besoin_accompagnement"])

    return df


# =============================================================
# Processing Questionnaire 2 : Retour sur l'utiliation de l'assistant ia
# =============================================================
def process_questionnaire_2(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["no_id"])
    df = df.drop_duplicates(subset="no_id", keep="last")
    return df


def process_questionnaire_2_typologie_interaction(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="types_d_interactions_mef")
    df = df.explode(column="types_d_interactions_mef")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["types_d_interactions_mef"])

    return df


def process_questionnaire_2_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_formation_ia_suivie_post_expe_")
    df = df.explode(column="id_formation_ia_suivie_post_expe_")
    df = df.dropna(subset=["id_formation_ia_suivie_post_expe_"])
    df = df.drop_duplicates()

    return df


def process_questionnaire_2_participation(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_participation_programme_rdv")
    df = df.explode(column="id_participation_programme_rdv")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_participation_programme_rdv"])
    df = df.drop_duplicates()

    return df


def process_questionnaire_2_freins(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_freins_a_l_utilisation")
    df = df.explode(column="id_freins_a_l_utilisation")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_freins_a_l_utilisation"])

    return df


def process_questionnaire_2_facteurs_progression(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_facteurs_de_progression")
    df = df.explode(column="id_facteurs_de_progression")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_facteurs_de_progression"])

    return df


def process_questionnaire_2_taches(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_taches_realisees_avec_ia")
    df = df.explode(column="id_taches_realisees_avec_ia")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_taches_realisees_avec_ia"])

    return df


def process_questionnaire_2_typologie_erreurs(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_types_d_erreurs_frequentes2")
    df = df.explode(column="id_types_d_erreurs_frequentes2")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_types_d_erreurs_frequentes2"])

    return df


def process_questionnaire_2_impact_observe(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_observations_des_impacts")
    df = df.explode(column="id_observations_des_impacts")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_observations_des_impacts"])

    return df


def process_questionnaire_2_impact_identifie(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_impacts_identifies_au_travail")
    df = df.explode(column="id_impacts_identifies_au_travail")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_impacts_identifies_au_travail"])

    return df


# =============================================================
# Processing du questionnaire2_bis : Les agents qui ne se sont jamais connectés
# =============================================================
def process_questionnaire_2_bis(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_questionnaire_2_bis_raisons_non_utilisation(
    df: pd.DataFrame,
) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_raisons_non_utilisation")
    df = df.explode(column="id_raisons_non_utilisation")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_raisons_non_utilisation"])

    return df


# =============================================================
# Processing du questionnaire 3 : Usages et ressentis face à l'Assistant IA, en fin de phase de test
# =============================================================
def process_questionnaire_3(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="no_id", keep="last")
    return df


def process_questionnaire_3_formation_suivie(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_formation_suivie")
    df = df.explode(column="id_formation_suivie")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_formation_suivie"])

    return df


def process_questionnaire_3_programme_rdv(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_programme_de_rdv")
    df = df.explode(column="id_programme_de_rdv")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["id_programme_de_rdv"])

    return df


def process_questionnaire_3_leviers_progression(df: pd.DataFrame) -> pd.DataFrame:
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_leviers_progression")
    df = df.explode(column="id_leviers_progression")
    # Nettoyage
    df = df.dropna(subset=["id_leviers_progression"])

    return df


def process_questionnaire_3_fonctionnalites(df: pd.DataFrame) -> pd.DataFrame:
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_fonctionnalites")
    df = df.explode(column="id_fonctionnalites")
    # Nettoyage
    df = df.dropna(subset=["id_fonctionnalites"])

    return df


def process_questionnaire_3_risques_identifies(df: pd.DataFrame) -> pd.DataFrame:
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_risques_identifies")
    df = df.explode(column="id_risques_identifies")
    # Nettoyage
    df = df.dropna(subset=["id_risques_identifies"])

    return df


def process_questionnaire_3_besoins_prioritaires(df: pd.DataFrame) -> pd.DataFrame:
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_besoins_prioritaires")
    df = df.explode(column="id_besoins_prioritaires")
    # Nettoyage
    df = df.dropna(subset=["id_besoins_prioritaires"])

    return df


def process_questionnaire_3_besoins_moindres(df: pd.DataFrame) -> pd.DataFrame:
    # Conversion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_besoins_moindres")
    df = df.explode(column="id_besoins_moindres")
    # Nettoyage
    df = df.dropna(subset=["id_besoins_moindres"])

    return df
