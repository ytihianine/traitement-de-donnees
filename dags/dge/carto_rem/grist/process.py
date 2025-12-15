import pandas as pd
import numpy as np


"""
    Functions de processing des référentiels
"""


def process_ref_position(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"niveau_diplome": "id_niveau_diplome"})
    df["id_niveau_diplome"].replace(0, np.nan, inplace=True)
    return df


def process_ref_categorie_ecole(df: pd.DataFrame) -> pd.DataFrame:
    df["categorie_d_ecole"] = (
        df["categorie_d_ecole"].str.strip().str.split().str.join(" ")
    )
    return df


def process_ref_libelle_diplome(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "categorie_ecole": "id_categorie_ecole",
            "niveau_diplome_associe": "id_niveau_diplome_associe",
        }
    )
    df["libelle_diplome"] = df["libelle_diplome"].str.strip().str.split().str.join(" ")
    df["id_categorie_ecole"].replace(0, np.nan, inplace=True)
    df["id_niveau_diplome_associe"].replace(0, np.nan, inplace=True)
    return df


def process_ref_base_remuneration(df: pd.DataFrame) -> pd.DataFrame:
    df["base_remuneration"] = (
        df["base_remuneration"].str.strip().str.split().str.join(" ")
    )
    return df


def process_ref_base_revalorisation(df: pd.DataFrame) -> pd.DataFrame:
    df["base_revalorisation"] = (
        df["base_revalorisation"].str.strip().str.split().str.join(" ")
    )
    return df


def process_ref_experience_pro(df: pd.DataFrame) -> pd.DataFrame:
    df["experience_pro"] = df["experience_pro"].str.strip().str.split().str.join(" ")
    return df


def process_ref_niveau_diplome(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["label_selection"])
    df["niveau_diplome"] = df["niveau_diplome"].str.strip().str.split().str.join(" ")
    return df


def process_ref_valeur_point_indice(df: pd.DataFrame) -> pd.DataFrame:
    df["date_d_application"] = pd.to_datetime(df["date_d_application"], unit="s")
    return df


"""
    Functions de processing des Grist source
"""


def process_agent_diplome(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent", "id"])
    df = df.rename(
        columns={
            "niveau_diplome_associe": "id_niveau_diplome_associe",
            "categorie_d_ecole": "id_categorie_d_ecole",
            "libelle_diplome": "id_libelle_diplome",
        }
    )
    df["id_libelle_diplome"] = df["id_libelle_diplome"].replace({0: None})
    df["id_niveau_diplome_associe"] = df["id_niveau_diplome_associe"].replace({0: None})
    df["id_categorie_d_ecole"] = df["id_categorie_d_ecole"].replace({0: None})
    # df = df.reset_index(drop=True)
    # df["id"] = df.index
    return df


def process_agent_revalorisation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent", "id"])
    df = df.rename(columns={"base_revalorisation": "id_base_revalorisation"})
    df["historique"] = df["historique"].str.strip().str.split().str.join(" ")
    date_cols = ["date_dernier_renouvellement", "date_derniere_revalorisation"]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], unit="s", errors="coerce")
    df["id_base_revalorisation"] = df["id_base_revalorisation"].replace({0: None})
    df = df.loc[df["matricule_agent"] != 0]
    # df = df.reset_index(drop=True)
    # df["id"] = df.index
    return df


def process_agent_contrat_grist(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent", "duree_contrat_en_cours_auto", "id"])
    # df["duree_cumulee_contrats_tout_contrat_mef"] = (
    #     df["duree_cumulee_contrats_tout_contrat_mef"]
    #     .apply(lambda x: x.decode("utf-8") if isinstance(x, bytes) else x)
    #     .astype(str)
    #     .str.strip()
    #     .str.split()
    #     .str.join(" ")
    #     .fillna("")
    # )
    df = df.rename(columns={"date_d_entree_a_la_dge": "date_entree_dge"})
    date_cols = [
        "date_premier_contrat_mef",
        "date_debut_contrat_actuel_dge",
        "date_entree_dge",
        "date_fin_contrat_cdd_en_cours_au_soir",
        "date_de_cdisation",
    ]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], unit="s")
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.replace("\x00", "", regex=False)
    # df = df.reset_index(drop=True)
    # df["id"] = df.index
    return df


def process_agent_rem_variable(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent"])
    df = df.rename(
        columns={
            "part_variable_collective": "plafond_part_variable_collective",
            "base_remuneration": "id_base_remuneration",
        }
    )
    df["observations"] = df["observations"].str.strip().str.split().str.join(" ")
    df["id_base_remuneration"].replace(0, np.nan, inplace=True)
    return df


def process_agent_experience_pro(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent", "id", "int_niv_diplome"])
    df = df.rename(columns={"position_grille": "id_position_grille"})
    # Convertir les mois en valeurs décimales
    df["exp_pro_totale_mois"] = (df["exp_pro_totale_mois"] / 12).round(1)
    df["exp_qualifiante_sur_le_poste_mois"] = (
        df["exp_qualifiante_sur_le_poste_mois"] / 12
    ).round(1)

    # Calculer l'exp pro en année
    df["experience_pro_totale"] = (
        df.loc[:, "exp_pro_totale_annee"] + df.loc[:, "exp_pro_totale_mois"]
    )
    df["experience_pro_qualifiante_sur_poste"] = (
        df.loc[:, "exp_qualifiante_sur_le_poste_annee"]
        + df.loc[:, "exp_qualifiante_sur_le_poste_mois"]
    )

    df = df.loc[df["matricule_agent"] != 0]
    df["id_position_grille"].replace(0, np.nan, inplace=True)
    # df = df.reset_index(drop=True)
    # df["id"] = df.index
    return df
