import logging

import pandas as pd
from modules.constants import NO_PROCESS_MSG


# ======================================================
# Référentiels
# ======================================================
def process_ref_prog(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_bop(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_uo(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_cc(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_sp_pilotage(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_sp_choisi(df: pd.DataFrame) -> pd.DataFrame:
    # Fill null values
    df["mail"] = df["mail"].fillna("Indéfini")
    return df


def process_ref_sdep(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# ======================================================
# Services prescripteurs
# ======================================================
def process_sp(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["centre_financier", "centre_cout"], how="any")
    # Drop columns
    cols_to_drop = ["import_timestamp"]
    df = df.drop(columns=cols_to_drop)
    df["doublon"] = df["doublon"].replace({0: False, 1: True})
    return df


# ======================================================
# Services prescripteurs - Renseignés manuellement
# ======================================================
def process_delai_global_paiement_sp_manuel(df: pd.DataFrame) -> pd.DataFrame:
    # Drop columns
    cols_to_drop = [
        "centre_de_cout",
        "centre_financier",
        "annee_exercice",
        "societe",
        "type_piece",
        "import_timestamp",
    ]
    df = df.drop(columns=cols_to_drop)

    return df


def process_demande_achat_sp_manuel(df: pd.DataFrame) -> pd.DataFrame:
    # Drop columns
    cols_to_drop = [
        "centre_de_cout",
        "centre_financier",
        "import_timestamp",
        "unique_multi",
    ]
    df = df.drop(columns=cols_to_drop)

    return df


def process_demande_paiement_sp_manuel(df: pd.DataFrame) -> pd.DataFrame:
    # Drop columns
    cols_to_drop = [
        "centre_de_cout",
        "centre_financier",
        "unique_multiple",
        "texte_de_poste",
        "import_timestamp",
    ]
    df = df.drop(columns=cols_to_drop)

    return df


def process_engagement_juridique_sp_manuel(df: pd.DataFrame) -> pd.DataFrame:
    # Drop columns
    cols_to_drop = [
        "centre_de_cout",
        "centre_financier",
        "orga",
        "gac",
        "import_timestamp",
    ]
    df = df.drop(columns=cols_to_drop)

    return df
