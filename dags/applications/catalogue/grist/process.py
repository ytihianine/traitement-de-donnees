import pandas as pd

from utils.config.vars import NO_PROCESS_MSG
from utils.control.dates import convert_grist_date_to_date
from utils.control.structures import handle_grist_null_references
from utils.control.text import normalize_whitespace_columns


# ======================================================
# Référentiels
# ======================================================
def process_ref_informationsystem(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["si", "commentaire"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_organisation(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["nom", "commentaire"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_service(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"organisation": "id_organisation"})

    txt_cols = ["nom", "acronyme"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    ref_cols = ["id_organisation"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_ref_people(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"service": "id_service"})

    txt_cols = ["contact", "mail", "commentaire", "profil"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    ref_cols = ["id_service"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_ref_format(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["format"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_licence(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["licence", "id_technique"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_frequency(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["frequence", "id_technique"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_geographicalcoverage(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["couverture_geographique", "id_technique"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_theme(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["theme"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_contactpoint(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"service": "id_service"})

    txt_cols = ["nom_bureau", "mail", "commentaire"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    ref_cols = ["id_service"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_ref_catalogue(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_typedonnees(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["type_donnee"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


# ======================================================
# Catalogue
# ======================================================
def process_catalogue(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "organisation": "id_organisation",
            "service": "id_service",
            "systeme_d_information": "id_systeme_information",
            "contact_service": "id_contactpoint",
            "frequence_maj": "id_frequency",
            "couverture_geo": "id_geographicalcoverage",
            "licence": "id_licence",
            "theme": "id_theme",
            "format": "id_format",
        },
        errors="raise",
    )

    txt_cols = [
        "schema_name",
        "table_name",
        "titre",
        "description",
        "couverture_temporelle",
        "url",
        "url_open_data",
        "mail_contact_service",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    ref_cols = [
        "id_organisation",
        "id_service",
        "id_systeme_information",
        "id_contactpoint",
        "id_frequency",
        "id_geographicalcoverage",
        "id_licence",
    ]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    date_cols = ["created_at", "updated_at"]
    df = convert_grist_date_to_date(df=df, columns=date_cols)

    return df


def process_dictionnaire(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "jeu_de_donnees": "id_catalogue",
            "type": "id_typedonnees",
            "service": "id_service",
        },
        errors="raise",
    )

    txt_cols = ["variable", "unite", "commentaire", "schema_name", "table_name"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    date_cols = ["created_at", "updated_at"]
    df = convert_grist_date_to_date(df=df, columns=date_cols)

    ref_cols = ["id_catalogue", "id_typedonnees", "id_service"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df
