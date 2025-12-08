from datetime import datetime
import pandas as pd


# ======================================================
# Process db catalogue
# ======================================================
def pg_info_extract_catalogue(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = ["table_schema", "table_name"]
    df = df.loc[:, cols_to_keep]

    # Remove duplicate
    df = df.drop_duplicates(subset=cols_to_keep)

    # Process
    df = process_catalogue(df=df)

    return df


def pg_info_extract_dictionnaire(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"column_name": "variable", "data_type": "type"})

    cols_to_keep = ["table_schema", "table_name", "variable", "type"]
    df = df.loc[:, cols_to_keep]

    # Process
    df = process_dictionnaire(df=df)

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

    return df.loc[df["indicator"] == "right_only"]


def compare_dictionnaire(df_db: pd.DataFrame, df_grist) -> pd.DataFrame:
    cols_to_keep = ["schema_name", "table_name", "variable"]
    df_grist = df_grist.loc[:, cols_to_keep]

    df = pd.merge(
        left=df_grist, right=df_db, on=cols_to_keep, how="outer", indicator=True
    )

    return df.loc[df["indicator"] == "right_only"]


# ======================================================
# Process new rows
# ======================================================
def process_catalogue(df: pd.DataFrame) -> pd.DataFrame:
    # Renommer les colonnes
    df = df.rename(
        columns={
            "table_schema": "schema_name",
            "table_name": "table_name",
        }
    )

    # Ajouter les colonnes additionnelles
    df["Public"] = False
    df["Est_visible"] = False
    df["Titre"] = df.loc[:, "table_name"].str.split("_").str.join(" ").str.capitalize()
    date_cols = ["created_at", "updated_at"]
    df[date_cols] = datetime.now()

    return df


def process_dictionnaire(df: pd.DataFrame) -> pd.DataFrame:
    # Renommer les colonnes
    df = df.rename(
        columns={
            "table_schema": "schema_name",
            "table_name": "table_name",
        }
    )

    return df
