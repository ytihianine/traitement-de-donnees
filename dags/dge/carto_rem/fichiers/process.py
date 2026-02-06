import pandas as pd

from utils.control.text import convert_str_cols_to_date, normalize_whitespace_columns


# ================================================
#   Functions de processing des fichiers sources
# ================================================
def process_agent_info_carriere(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["dge_perimetre", "nom_usuel", "prenom"]
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


"""
    Functions de processing des fichiers finaux
"""


def process_agent(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only needed columns
    cols_to_keep = [
        "matricule_agent",
        "nom_usuel",
        "prenom",
        "genre",
        "age",
        "dge_perimetre",
    ]
    df = df.loc[:, cols_to_keep]

    return df


def process_agent_carriere(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only needed columns
    cols_to_keep = [
        "matricule_agent",
        "categorie",
        "qualite_statutaire",
        "corps",
        "grade",
        "echelon",
        "indice_majore",
    ]
    df = df.loc[:, cols_to_keep]

    # Convert column
    df["echelon"] = pd.to_numeric(arg=df["echelon"])

    return df
