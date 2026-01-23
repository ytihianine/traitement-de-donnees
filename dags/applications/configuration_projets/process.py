from enums.dags import TypeDocumentation
from enums.database import LoadStrategy, PartitionTimePeriod
import pandas as pd
import numpy as np
from utils.control.dates import convert_grist_date_to_date
from utils.control.structures import (
    handle_grist_boolean_columns,
    handle_grist_null_references,
    validate_enum_column,
)
from utils.control.text import normalize_whitespace_columns


def replace_values(
    df: pd.DataFrame, to_replace: dict[str, str], cols: list[str] | None = None
) -> pd.DataFrame:
    if cols:
        df[cols] = df[cols].replace(to_replace=to_replace)
    else:
        df = df.replace(to_replace=to_replace)
    return df


def process_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(direction=df["direction"].str.strip()).convert_dtypes()
    df = df.drop_duplicates(subset=["direction"])
    df = df.dropna(subset=["direction"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df.loc[:, sorted_cols]

    return df


def process_service(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {"direction": "id_direction"}

    df = (
        df.rename(columns=cols_to_rename)
        .assign(service=df["service"].str.strip())
        .convert_dtypes()
    )
    df = df.drop_duplicates(subset=["id_direction", "service"])
    df = df.dropna(subset=["id_direction", "service"])

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df.loc[:, sorted_cols]

    return df


def process_projet(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "direction": "id_direction",
        "service": "id_service",
    }
    df = df.rename(columns=cols_to_rename)

    # Réf colonnes
    ref_cols = ["id_direction", "id_service"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Txt colonnes
    txt_cols = ["projet"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    df = df.dropna(subset=["projet", "id_direction", "id_service"])

    return df


def process_projet_contact(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
    }
    df = df.rename(columns=cols_to_rename)

    # Réf colonnes
    ref_cols = ["id_projet"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Txt colonnes
    txt_cols = ["contact_mail"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Bool colonnes
    bool_cols = ["is_mail_generic"]
    df = handle_grist_boolean_columns(df=df, columns=bool_cols)

    # Retirer les lignes avec contact_mail vide (après normalisation)
    df = df.loc[df["contact_mail"].astype(bool)]

    return df


def process_projet_documentation(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
    }
    df = df.rename(columns=cols_to_rename)

    # Réf colonnes
    ref_cols = ["id_projet"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Txt colonnes
    txt_cols = ["type_documentation", "lien"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Check constraintes
    validate_enum_column(
        df=df,
        column="type_documentation",
        enum_class=TypeDocumentation,
        allow_null=False,
    )

    return df


def process_projet_s3(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
    }
    df = df.rename(columns=cols_to_rename)

    # Réf colonnes
    ref_cols = ["id_projet"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Txt colonnes
    txt_cols = ["bucket", "key", "key_tmp"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_selecteur(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
        "type_de_selecteur": "type_selecteur",
        "selecteur": "selecteur",
    }
    df = df.rename(columns=cols_to_rename)

    # Réf colonnes
    ref_cols = ["id_projet"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = df.dropna(subset=["id_projet"])

    # Txt colonnes
    txt_cols = ["selecteur"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_source(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
        "type": "type_source",
        "selecteur": "id_selecteur",
    }
    df = df.rename(columns=cols_to_rename)
    df = df.drop(columns=["sous_type"])

    # Réf colonnes
    ref_cols = ["id_projet", "id_selecteur"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = df.dropna(subset=["id_projet", "id_selecteur"])

    # Txt colonnes
    txt_cols = ["type_source", "id_source"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Retirer les lignes avec contact_mail vide (après normalisation)
    df = df.loc[df["id_source"].astype(bool)]

    return df


def process_selecteur_s3(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
        "selecteur": "id_selecteur",
    }
    df = df.rename(columns=cols_to_rename)

    # Réf colonnes
    ref_cols = ["id_projet", "id_selecteur"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Txt colonnes
    txt_cols = ["filename", "key"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_selecteur_database(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
        "selecteur": "id_selecteur",
    }
    df = df.rename(columns=cols_to_rename)

    # Réf colonnes
    ref_cols = ["id_projet", "id_selecteur"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Txt colonnes
    txt_cols = ["tbl_name", "partition_period", "load_strategy"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Bool colonnes
    bool_cols = ["is_partitionned"]
    df = handle_grist_boolean_columns(df=df, columns=bool_cols)

    # Check constraintes - Partition period
    validate_enum_column(
        df=df.loc[df["is_partitionned"]],
        column="partition_period",
        enum_class=PartitionTimePeriod,
        allow_null=False,
    )
    validate_enum_column(
        df=df, column="load_strategy", enum_class=LoadStrategy, allow_null=False
    )

    return df


def process_storage_path(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
        "selecteur": "id_selecteur",
        "db_tbl_name": "tbl_name",
    }
    df = (
        df.rename(columns=cols_to_rename)
        .pipe(replace_values, to_replace={0: None}, cols=["id_projet", "id_selecteur"])
        .assign(
            local_tmp_dir=df["local_tmp_dir"].str.strip(),
            s3_bucket=df["s3_bucket"].str.strip(),
            s3_key=df["s3_key"].str.strip(),
            s3_tmp_key=df["s3_tmp_key"].str.strip(),
        )
        .convert_dtypes()
    )
    df = df.dropna(subset=["id_projet", "id_selecteur"])
    df["tbl_name"] = df["tbl_name"].replace(r"^\s+$", np.nan, regex=True)

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df.loc[:, sorted_cols]

    return df


def process_col_mapping(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
        "selecteur": "id_selecteur",
    }
    df = df.rename(columns=cols_to_rename)
    df = df.drop(
        columns=[
            "nombre_d_utilisation",
            "commentaire",
            "statut",
            "nouvelle_proposition",
        ]
    )

    # Réf colonnes
    ref_cols = ["id_projet", "id_selecteur"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Bool colonnes
    bool_cols = ["to_keep"]
    df = handle_grist_boolean_columns(df=df, columns=bool_cols)

    # Txt colonnes
    txt_cols = ["colname_source", "colname_dest"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Date columns
    date_cols = ["date_archivage"]
    df = convert_grist_date_to_date(df=df, columns=date_cols)

    df = df.dropna(subset=["id_projet", "id_selecteur"])

    df = df.loc[df["to_keep"]]

    return df


def process_col_requises(df: pd.DataFrame) -> pd.DataFrame:
    # Rename
    cols_to_rename = {
        "projet": "id_projet",
        "selecteur": "id_selecteur",
        "colonne_requise": "id_correspondance_colonne",
    }
    col_to_keep = ["id", "id_projet", "id_selecteur", "id_correspondance_colonne"]
    df = (
        df.rename(columns=cols_to_rename)
        .pipe(
            replace_values,
            to_replace={0: None},
            cols=["id_projet", "id_selecteur", "id_correspondance_colonne"],
        )
        .convert_dtypes()
    )
    df = df.dropna(subset=["id_projet", "id_selecteur", "id_correspondance_colonne"])
    df = df.drop(columns=list(set(df.columns) - set(col_to_keep)))

    # Sort columns to match db cols order
    cols = df.columns
    sorted_cols = sorted(cols)
    df = df.loc[:, sorted_cols]

    return df
