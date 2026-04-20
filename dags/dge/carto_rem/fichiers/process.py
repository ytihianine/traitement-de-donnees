import pandas as pd

from utils.process.text import (
    convert_str_cols_to_date,
    normalize_whitespace_columns,
)


# ================================================
#   Functions de processing des fichiers sources
# ================================================
def process_agent_info_carriere(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["dge_perimetre", "nom_usuel", "prenom", "affectation_operationnelle"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


def process_agent_contrat(df: pd.DataFrame) -> pd.DataFrame:
    # Convert date columns
    date_cols = [
        "date_debut_contrat_actuel",
        "date_fin_contrat_previsionnelle_actuel",
        "date_cdisation",
    ]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%d/%m/%Y", errors="raise"
    )

    return df


def process_agent_r4(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["fonction_dge", "qualite_statutaire"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Keep only titulaires rows
    df = df.loc[df["qualite_statutaire"] == "T", :]

    # Keep only needed columns
    cols_to_keep = ["matricule_agent", "fonction_dge"]
    df = df.loc[:, cols_to_keep]

    return df
