import datetime
import logging

import pandas as pd

from project.constants import NO_PROCESS_MSG


def generate_date(year: int, semester: str) -> datetime.datetime | None:
    semester_values = {"S1": 6, "Total": 12}

    if year is None or semester is None:
        logging.info(msg="Either year or semester value is None.")
        return None

    if semester not in semester_values:
        logging.info(msg=f"Invalid semester value: {semester}. Must be one of {list(semester_values.keys())}")
        return None

    try:
        year = int(year)
        month = semester_values[semester]
        date = datetime.datetime(year, month, 1)
        return date
    except ValueError:
        logging.info(msg=f"year cannot be converted to int. Current year value: {year}")
        return None
    except Exception as e:
        logging.info(msg=f"An exception as occured: {e}")
        return None

    return None


# Traitement spécifique pour la table TELETRAVAIL
def process_teletravail(df: pd.DataFrame) -> pd.DataFrame:
    # Conversion en epoch time not working
    df["date"] = pd.to_datetime(df["date"], unit="s")
    return df


# Traitement spécifique pour la table teletravail_frequence
def process_teletravail_frequence(df: pd.DataFrame) -> pd.DataFrame:
    # Exemple : remplacer 'ND' par ''
    df.replace("ND", None, inplace=True)
    return df


# Traitement spécifique pour la table teletravail_frequence
def process_teletravail_opinion(df: pd.DataFrame) -> pd.DataFrame:
    # Exemple : remplacer 'ND' par ''
    df.replace("ND", None, inplace=True)
    return df


# Traitement spécifique pour Effectifs_MEFR_par_direction
def process_mef_par_direction(df: pd.DataFrame) -> pd.DataFrame:
    # Exemple : Suppression des colonnes vides et conversion de valeurs

    logging.info(msg="Traitement de la table Effectifs_MEFR_par_direction effectué.")
    return df


# Traitement spécifique pour Effectif_2022
def process_effectif_direction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Traitement spécifique pour Effectifs_par_perimetre
def process_effectif_perimetre(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Traitement spécifique pour Effectifs_par_d partements
def process_effectif_departements(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Traitement spécifique pour # Traitement spécifique pour Budget_total
def process_budget_total(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


def process_budget_pilotable(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Fonction pour Budget_General
def process_budget_general(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Fonction pour Evolution_budget_mef
def process_evolution_budget_mef(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Fonction pour Montant_intervention_invest
# def process_montant_invest(df: pd.DataFrame) -> pd.DataFrame:
#

#     logging.info(msg="Traitement de la table Montant_intervention_invest effectué.")
#     return df


def process_montant_intervention_invest(df: pd.DataFrame) -> pd.DataFrame:
    # Extraire l'année de la colonne 'source_montant' et la mettre dans 'annee'
    df["source_montant_split"] = df["source_montant"].str.split()
    df["annee"] = df["source_montant_split"].str.get(1)

    # Modifier la colonne 'source_montant' pour ne conserver que "LFI"
    df["source_montant"] = df["source_montant_split"].str.get(0)
    df = df.drop(columns=["source_montant_split"])
    return df


# Fonction pour Engagement_Agent
def process_engagement_agent(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Fonction pour RESULTAT_ELECTIONS
def process_election_resultat(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Fonction pour Taux_participation
def process_taux_participation(df: pd.DataFrame) -> pd.DataFrame:
    # Supprimer les colonnes
    df = df.drop(columns=["id"])

    # Unpivoter les colonnes d'années
    df_unpivoted = df.melt(
        id_vars=["secteur", "tri"],  # Colonnes à conserver telles quelles
        var_name="annee",  # Nom de la nouvelle colonne pour les années
        value_name="taux_participation",  # Nom de la nouvelle colonne pour les valeurs
    )

    # Filtrer les lignes où 'valeur' est NaN
    df_unpivoted = df_unpivoted.dropna(subset=["annee", "taux_participation"])

    # Ajouter un id
    df_unpivoted["annee"] = df_unpivoted["annee"].str.replace("c", "", regex=False)
    df_unpivoted["id"] = df_unpivoted.sort_values(by=["annee", "tri"]).reset_index(drop=True).index
    return df_unpivoted


# Fonction pour plafond_etpt
def process_plafond_etpt(df: pd.DataFrame) -> pd.DataFrame:
    df["source"] = df["source"].str.split()
    df["annee"] = df["source"].str.get(1)
    df["source"] = df["source"].str.get(0)
    return df


# Fonction pour db_plafond_etpt
def process_db_plafond_etpt(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Fonction pour masse_salariale
def process_masse_salariale(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


# Fonction pour masse_salariale
def process_budget_ministere(df: pd.DataFrame) -> pd.DataFrame:
    num_cols = [
        "budget_general",
        "budget_annexe",
        "compte_affection_speciale",
        "compte_concours_financiers",
        "budget_total",
    ]
    # Conversion Mds euros en euros
    df[num_cols] = df[num_cols].fillna(0.0)
    df[num_cols] = df[num_cols].multiply(10**9).round(decimals=0)
    return df
