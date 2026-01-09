from typing import Any
from dags.sg.srh.mentorat_merci.enums import ColName
import pandas as pd


# ================
# Fichiers sources
# ================
def process_mentors(df: pd.DataFrame) -> pd.DataFrame:
    # Retirer les duplicats
    df = df.drop_duplicates(
        subset=[ColName.nom.value, ColName.prenom.value, ColName.direction.value]
    )
    # Retirer les lignes sans nom
    df = df.loc[df[ColName.nom.value].notna()]
    # Normaliser les départements
    df[ColName.departement.value] = df[ColName.departement.value].str.lower()

    return df


def process_mentores(df: pd.DataFrame) -> pd.DataFrame:
    # Retirer les duplicats
    df = df.drop_duplicates(
        subset=[ColName.nom.value, ColName.prenom.value, ColName.direction.value]
    )
    # Retirer les lignes sans nom
    df = df.loc[df[ColName.nom.value].notna()]
    # Normaliser les départements
    df[ColName.departement.value] = df[ColName.departement.value].str.lower()

    return df


# ================
# Binômes
# ================
def filter_by_direction(
    df: pd.DataFrame, direction: str, same_direction: bool = False
) -> pd.DataFrame:
    if same_direction:
        return df.loc[df[ColName.direction.value] == direction]
    return df.loc[df[ColName.direction.value] != direction]


def filter_by_departement(df: pd.DataFrame, departement: str) -> pd.DataFrame:
    return df.loc[df[ColName.departement.value] == departement]


def filter_by_statut(df: pd.DataFrame, statut: str) -> pd.DataFrame:
    return df.loc[df[ColName.statut.value] == statut]


def filter_by_categorie(df: pd.DataFrame, categories: list[str]) -> pd.DataFrame:
    return df.loc[df[ColName.categorie.value].isin(categories)]


def filtrer_par_critere(
    df_to_filter: pd.DataFrame, criteres: dict[str, Any]
) -> pd.DataFrame:
    liste_critere = list(criteres.keys())

    if ColName.departement.value in liste_critere:
        df_to_filter = filter_by_departement(
            df=df_to_filter, departement=criteres[ColName.departement.value]
        )

    if ColName.statut.value in liste_critere:
        df_to_filter = filter_by_statut(
            df=df_to_filter, statut=criteres[ColName.statut.value]
        )

    if ColName.categorie.value in liste_critere:
        df_to_filter = filter_by_categorie(
            df=df_to_filter, categories=criteres[ColName.categorie.value]
        )

    return df_to_filter
