from numpy import float64
import pandas as pd

from utils.control.text import convert_str_cols_to_date, normalize_whitespace_columns


"""
    Functions de processing des fichiers sources
"""


def process_agent_elem_rem(df: pd.DataFrame) -> pd.DataFrame:
    # Clean columns
    df["matricule_agent"] = df["matricule_agent"].astype("int64")

    txt_cols = ["libelle_element", "uuid"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Filter les lignes
    elems_to_keep = [
        "TRAITEMENT BRUT",
        "INDEMNITE DE RESIDENCE",
        "IND. MENSUELLE TECHNICITE",
        "I.F.S.E.",
        "IND. COMPENSATRICE CSG",
    ]
    df = df.loc[df["libelle_element"].isin(elems_to_keep)]

    # Data processing
    df["montant_a_payer"] = (
        df["montant_a_payer"].str.replace(",", ".", regex=False).astype(float64)
    )
    df["montant_a_deduire"] = (
        df["montant_a_deduire"].str.replace(",", ".", regex=False).astype(float64)
    )

    return df


def process_agent_info_carriere(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["dge_perimetre", "nom_usuel", "prenom", "echelon"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


def process_agent_contrat(df: pd.DataFrame) -> pd.DataFrame:
    # Convert columns
    date_cols = ["date_debut_contrat_actuel", "date_fin_contrat_previsionnelle_actuel"]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%d/%m/%Y", errors="raise"
    )

    # Init columns
    cols_to_init = [
        "date_premier_contrat_mef",
        "duree_contrat_en_cours_dge",
        "duree_cumulee_contrats_tout_contrat_mef",
        "date_de_cdisation",
        "duree_en_jours_si_coupure_de_contrat",
        "date_entree_dge",
        "id_fonction_dge",
    ]
    df[cols_to_init] = pd.NA

    return df


"""
    Functions de processing des fichiers finaux
"""


def process_agent(df_agent_info_carriere: pd.DataFrame) -> pd.DataFrame:
    # Keep only needed columns
    cols_to_keep = [
        "matricule_agent",
        "nom_usuel",
        "prenom",
        "genre",
        "age",
    ]
    df = df_agent_info_carriere.loc[:, cols_to_keep]

    return df


def process_agent_carriere(df_agent_info_carriere: pd.DataFrame) -> pd.DataFrame:
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
    df = df_agent_info_carriere.loc[:, cols_to_keep]

    # Convert column
    df["echelon"] = pd.to_numeric(arg=df["echelon"])

    return df
