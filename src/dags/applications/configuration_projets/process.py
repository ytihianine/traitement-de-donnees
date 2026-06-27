import logging
import pandas as pd

from src.utils.process.structures import (
    validate_enum_column,
)
from src._enums.dags import TypeDocumentation
from src.constants import NO_PROCESS_MSG


def replace_values(
    df: pd.DataFrame, to_replace: dict[str, str], cols: list[str] | None = None
) -> pd.DataFrame:
    if cols:
        df[cols] = df[cols].replace(to_replace=to_replace)
    else:
        df = df.replace(to_replace=to_replace)
    return df


def process_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["direction"])
    df = df.dropna(subset=["direction"])
    return df


def process_service(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["id_direction", "service"])
    df = df.dropna(subset=["id_direction", "service"])
    return df


def process_projets(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["projet", "id_direction", "id_service"])
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


def process_projet_s3(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


def process_projet_selecteur(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["id_projet"])
    return df


def process_selecteur_source(df: pd.DataFrame) -> pd.DataFrame:
    # Retirer les lignes sans id_source (après normalisation)
    df = df.loc[df["id_source"].astype(bool)]
    return df


def process_selecteur_s3(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


def process_selecteur_database(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(NO_PROCESS_MSG)
    return df


def process_selecteur_column_mapping(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["id_projet", "id_selecteur"])
    return df
