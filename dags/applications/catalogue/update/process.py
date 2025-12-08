from datetime import datetime
import pandas as pd


# ======================================================
# Process db catalogue
# ======================================================
def pg_info_extract_catalogue(df: pd.DataFrame) -> pd.DataFrame:
    # Default processing
    df = df.rename(
        columns={
            "table_schema": "schema_name",
            "table_name": "table_name",
        }
    )

    cols_to_keep = ["schema_name", "table_name"]
    df = df.loc[:, cols_to_keep]

    # Remove duplicate
    df = df.drop_duplicates(subset=cols_to_keep)

    return df


def pg_info_extract_dictionnaire(df: pd.DataFrame) -> pd.DataFrame:
    # Default processing
    df = df.rename(
        columns={
            "table_schema": "schema_name",
            "table_name": "table_name",
            "column_name": "variable",
            "data_type": "type",
        }
    )

    cols_to_keep = ["schema_name", "table_name", "variable", "type"]
    df = df.loc[:, cols_to_keep]

    # Remove duplicate
    df = df.drop_duplicates(subset=cols_to_keep)

    return df


# ======================================================
# Comparaison
# ======================================================
def compare_catalogue(df_db: pd.DataFrame, df_grist) -> pd.DataFrame:
    cols_to_keep = ["schema_name", "table_name"]
    df_grist = df_grist.loc[:, cols_to_keep]

    df = pd.merge(
        left=df_grist, right=df_db, on=cols_to_keep, how="outer", indicator=True
    )

    # Filter
    df = df.loc[df["_merge"] == "right_only"]
    df = df.drop(columns=["_merge"])

    return df


def compare_dictionnaire(df_db: pd.DataFrame, df_grist) -> pd.DataFrame:
    cols_to_keep = ["schema_name", "table_name", "variable"]
    df_grist = df_grist.loc[:, cols_to_keep]

    df = pd.merge(
        left=df_grist, right=df_db, on=cols_to_keep, how="outer", indicator=True
    )

    # Filter
    df = df.loc[df["_merge"] == "right_only"]
    df = df.drop(columns=["_merge"])

    return df


# ======================================================
# Process new rows
# ======================================================
def process_catalogue(df: pd.DataFrame) -> pd.DataFrame:
    # Ajouter les colonnes additionnelles
    df["Public"] = False
    df["Est_visible"] = False
    df["Titre"] = df.loc[:, "table_name"].str.split("_").str.join(" ").str.capitalize()
    date_cols = ["created_at", "updated_at"]
    df[date_cols] = datetime.now().timestamp()

    return df


def process_dictionnaire(df: pd.DataFrame) -> pd.DataFrame:
    # Correspondance des types

    return df
