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


# ====================
# Tables de dimension
# ====================
def process_dim_projet(
    df_projet: pd.DataFrame,
    df_ref_direction: pd.DataFrame,
    df_ref_service: pd.DataFrame,
    df_projet_location: pd.DataFrame,
) -> pd.DataFrame:
    metadata_cols = ["id_row", "snapshot_id_parent", "snapshot_id", "import_timestamp"]
    df_projet_clean = df_projet.drop(columns=metadata_cols, errors="ignore")
    df_projet_location_clean = df_projet_location.drop(columns=metadata_cols, errors="ignore")
    df_ref_direction_clean = df_ref_direction.drop(columns=metadata_cols, errors="ignore")
    df_ref_service_clean = df_ref_service.drop(columns=["id_direction", *metadata_cols], errors="ignore")

    df_dim_projet = (
        df_projet_clean.merge(
            right=df_projet_location_clean,
            how="left",
            left_on="id_projet",
            right_on="id_projet",
        )
        .merge(
            right=df_ref_direction_clean,
            how="left",
            left_on="id_direction",
            right_on="id_direction",
        )
        .merge(
            right=df_ref_service_clean,
            how="left",
            left_on="id_service",
            right_on="id_service",
        )
        .drop_duplicates(subset=["id_projet"])
    )
    return df_dim_projet


def process_dim_projet_contact(df_projet: pd.DataFrame, df_projet_contact: pd.DataFrame) -> pd.DataFrame:
    metadata_cols = ["id_row", "snapshot_id_parent", "snapshot_id", "import_timestamp"]
    df_projet_clean = df_projet.drop(columns=metadata_cols, errors="ignore")
    df_projet_contact_clean = df_projet_contact.drop(columns=metadata_cols, errors="ignore")

    df_dim_projet_contact = (
        df_projet_clean.merge(
            right=df_projet_contact_clean,
            how="left",
            left_on="id_projet",
            right_on="id_projet",
        )
        .drop(columns=["id_direction", "id_service"])
        .drop_duplicates(subset=["id_projet", "id_contact"])
    )

    if "id_contact" in df_dim_projet_contact.columns:
        df_dim_projet_contact["id_contact"] = pd.to_numeric(df_dim_projet_contact["id_contact"], errors="coerce")
        df_dim_projet_contact = df_dim_projet_contact.astype({"id_contact": "Int64"})

    return df_dim_projet_contact


def process_dim_projet_documentation(df_projet: pd.DataFrame, df_projet_documentation: pd.DataFrame) -> pd.DataFrame:
    metadata_cols = ["id_row", "snapshot_id_parent", "snapshot_id", "import_timestamp"]
    df_projet_clean = df_projet.drop(columns=metadata_cols, errors="ignore")
    df_projet_documentation_clean = df_projet_documentation.drop(columns=metadata_cols, errors="ignore")

    df_dim_projet_documentation = (
        df_projet_clean.merge(
            right=df_projet_documentation_clean,
            how="left",
            left_on="id_projet",
            right_on="id_projet",
        )
        .drop(columns=["id_direction", "id_service"])
        .drop_duplicates(subset=["id_projet", "type_documentation"])
    )
    return df_dim_projet_documentation


def process_dim_dataset(
    df_projet: pd.DataFrame, df_dataset: pd.DataFrame, df_ref_direction: pd.DataFrame, df_ref_service: pd.DataFrame
) -> pd.DataFrame:
    metadata_cols = ["id_row", "snapshot_id_parent", "snapshot_id", "import_timestamp"]
    df_projet_clean = df_projet.drop(columns=metadata_cols, errors="ignore")
    df_dataset_clean = df_dataset.drop(columns=metadata_cols, errors="ignore")
    df_ref_direction_clean = df_ref_direction.drop(columns=metadata_cols, errors="ignore")
    df_ref_service_clean = df_ref_service.drop(columns=["id_direction", *metadata_cols], errors="ignore")

    df_dim_dataset = (
        df_projet_clean.merge(
            right=df_dataset_clean,
            how="left",
            left_on="id_projet",
            right_on="id_projet",
        )
        .merge(
            right=df_ref_direction_clean,
            how="left",
            left_on="id_direction",
            right_on="id_direction",
        )
        .merge(
            right=df_ref_service_clean,
            how="left",
            left_on="id_service",
            right_on="id_service",
        )
        .drop_duplicates(subset=["id_projet", "dataset"])
    )
    return df_dim_dataset


def process_dim_dataset_location(
    df_projet: pd.DataFrame,
    df_dataset: pd.DataFrame,
    df_dataset_location: pd.DataFrame,
    df_ref_type_location: pd.DataFrame,
    df_ref_connexion: pd.DataFrame,
) -> pd.DataFrame:
    metadata_cols = ["id_row", "snapshot_id_parent", "snapshot_id", "import_timestamp"]
    df_projet_clean = df_projet.drop(columns=metadata_cols, errors="ignore")
    df_dataset_clean = df_dataset.drop(columns=metadata_cols, errors="ignore")
    df_dataset_location_clean = df_dataset_location.drop(columns=metadata_cols, errors="ignore")
    df_ref_type_location_clean = df_ref_type_location.drop(columns=metadata_cols, errors="ignore")
    df_ref_connexion_clean = df_ref_connexion.drop(columns=metadata_cols, errors="ignore")

    df_dim_dataset_location = (
        df_projet_clean.merge(
            right=df_dataset_clean,
            how="left",
            left_on="id_projet",
            right_on="id_projet",
        )
        .merge(
            right=df_dataset_location_clean,
            how="left",
            left_on=["id_projet", "id_dataset"],
            right_on=["id_projet", "id_dataset"],
        )
        .merge(
            right=df_ref_type_location_clean,
            how="left",
            left_on="id_type_location",
            right_on="id_type_location",
        )
        .merge(
            right=df_ref_connexion_clean,
            how="left",
            left_on=["id_type_location", "id_conn_id"],
            right_on=["id_type_location", "id_connexion"],
        )
        .drop(columns=["id_connexion"], errors="ignore")
        .drop(columns=["id_direction", "id_service"])
        .drop_duplicates(subset=["id_projet", "dataset", "stage"])
    )
    return df_dim_dataset_location


def process_dim_dataset_column_mapping(
    df_projet: pd.DataFrame, df_dataset: pd.DataFrame, df_dataset_column_mapping: pd.DataFrame
) -> pd.DataFrame:
    metadata_cols = ["id_row", "snapshot_id_parent", "snapshot_id", "import_timestamp"]
    df_projet_clean = df_projet.drop(columns=metadata_cols, errors="ignore")
    df_dataset_clean = df_dataset.drop(columns=metadata_cols, errors="ignore")
    df_dataset_column_mapping_clean = df_dataset_column_mapping.drop(columns=metadata_cols, errors="ignore")

    df_dim_dataset_column_mapping = (
        df_projet_clean.merge(
            right=df_dataset_clean,
            how="left",
            left_on="id_projet",
            right_on="id_projet",
        )
        .merge(
            right=df_dataset_column_mapping_clean,
            how="left",
            left_on=["id_projet", "id_dataset"],
            right_on=["id_projet", "id_dataset"],
        )
        .drop(columns=["id_direction", "id_service"])
        .drop_duplicates(subset=["id_projet", "id_dataset", "colname_source"])
    )
    return df_dim_dataset_column_mapping
