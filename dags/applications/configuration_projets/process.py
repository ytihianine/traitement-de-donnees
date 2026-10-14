import pandas as pd
from modules.domain.projet.model import TypeDocumentation
from modules.generic_processing.structures import (
    validate_enum_column,
)


# ====================
# Référentiels
# ====================
def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["direction"])
    df = df.dropna(subset=["direction"])
    return df


def process_ref_service(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["id_direction", "service"])
    df = df.dropna(subset=["id_direction", "service"])
    return df


def process_ref_type_location(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["type_location"])
    df = df.dropna(subset=["type_location"])
    return df


def process_ref_connexion(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["id_type_location", "conn_id"])
    df = df.dropna(subset=["id_type_location", "conn_id"])
    return df


# ====================
# Tables métiers
# ====================
def process_projet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["projet", "id_direction", "id_service"])
    return df


def process_projet_location(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_projet_contact(df: pd.DataFrame) -> pd.DataFrame:
    # Retirer les lignes avec contact_mail vide (après normalisation)
    df = df.loc[df["contact_mail"].astype(bool)]
    return df


def process_projet_documentation(df: pd.DataFrame) -> pd.DataFrame:
    # Check constraintes
    validate_enum_column(
        df=df,
        column="type_documentation",
        enum_class=TypeDocumentation,
        allow_null=False,
    )

    return df


def process_dataset(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["id_projet", "dataset"])
    df = df.dropna(subset=["id_projet", "dataset"])
    return df


def process_dataset_location(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["id_projet", "id_dataset", "stage"])
    df = df.dropna(subset=["id_projet", "id_dataset", "stage", "id_type_location", "location"])
    return df


def process_dataset_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["id_projet", "id_dataset"])
    return df
