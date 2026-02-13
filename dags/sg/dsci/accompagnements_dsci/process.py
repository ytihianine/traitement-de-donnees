import logging
import pandas as pd

from utils.config.vars import NO_PROCESS_MSG
from utils.control.dates import convert_grist_date_to_date
from utils.control.structures import (
    convert_str_of_list_to_list,
    handle_grist_null_references,
)
from utils.control.text import normalize_whitespace_columns


"""
    Fonction de processing des référentiels
"""


def process_ref_certification(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_competence_particuliere(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_bureau(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_pole(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"bureau": "id_bureau"})
    return df


def process_ref_profil_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["profil_correspondant", "intitule_long", "created_by", "updated_by"]
    other_cols = ["id"]

    # Retirer les colonnes non-utiles
    cols_to_drop = list(set(df.columns) - set(txt_cols + other_cols))
    df = df.drop(columns=cols_to_drop)

    # Nettoyer les données
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_promotion_fac(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_semainier(df: pd.DataFrame) -> pd.DataFrame:
    df["date_semaine"] = pd.to_datetime(df["date_semaine"], unit="s")
    df = df.drop(columns=["is_duplicate"])
    return df


def process_ref_qualite_service(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_date_debut_inactivite(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_region(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_type_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"pole": "id_pole"})

    # Gérer les références
    ref_cols = ["id_pole"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_ref_typologie_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    df["typologie_accompagnement"] = (
        df["typologie_accompagnement"].str.strip().str.split().str.join(" ")
    )
    return df


"""
    Fonction de processing des données Mission Innovation
"""


def process_accompagnement_mi(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = [
        "id",
        "intitule",
        "direction",
        "date_de_realisation",
        "statut",
        "pole",
        "type_d_accompagnement",
        "est_certifiant",
        "places_max",
        "nb_inscrits",
        "places_restantes",
        "est_ouvert_notation",
        "informations_complementaires",
        "id_type_accompagnement",
    ]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "direction": "id_direction",
            "pole": "id_pole",
            "type_d_accompagnement": "id_type_d_accompagnement",
        }
    )
    df["date_de_realisation"] = pd.to_datetime(df["date_de_realisation"], unit="s")
    df["informations_complementaires"] = (
        df["informations_complementaires"].str.strip().str.split().str.join(" ")
    )

    # Gérer les références
    ref_cols = ["id_direction", "id_pole", "id_type_d_accompagnement"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_accompagnement_mi_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "accompagnement": "id_accompagnement",
            "type_d_accompagnement": "id_type_d_accompagnement",
        }
    )

    # Gérer les références
    ref_cols = ["id_accompagnement", "id_type_d_accompagnement"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


"""
    Fonction de processing des données
"""


def process_accompagnement_dsci(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_accompagnement_opportunite_cci(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_animateur_externe(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_animateur_fac(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_animateur_interne(df: pd.DataFrame) -> pd.DataFrame:
    return df


"""
    Fonction de processing des bilatérales
"""


def process_bilaterale(df: pd.DataFrame) -> pd.DataFrame:
    df["date_de_rencontre"] = pd.to_datetime(df["date_de_rencontre"], unit="s")
    df = df.rename(columns={"direction": "id_direction"})
    return df


def process_bilaterale_remontee(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["int_direction"])
    df = df.rename(
        columns={
            "bilaterale": "id_bilaterale",
            "bureau": "id_bureau",
        }
    )
    df["information_a_remonter"] = (
        df["information_a_remonter"].str.strip().str.split().str.join(" ")
    )

    # Gérer les références
    ref_cols = ["id_bilaterale", "id_bureau"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


"""
    Fonction de processing des correspondants
"""


def process_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = [
        "id",
        "mail",
        "nom_complet",
        "direction",
        "entite",
        "region",
        "actif",
        "promotion_fac",
        "est_certifie_fac",
        "actif_communaute_fac",
        "direction_hors_mef",
        "prenom",
        "nom",
        "date_debut_inactivite",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "promotion_fac": "id_promotion_fac",
        }
    )

    # Convert date
    date_cols = ["date_debut_inactivite"]
    df = convert_grist_date_to_date(df=df, columns=date_cols)

    # Cleaning
    txt_cols = ["mail", "entite", "direction_hors_mef", "prenom", "nom", "nom_complet"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    df["entite"] = df["entite"].replace('"', "", regex=False)

    # Filtrer les lignes
    # rows_to_drop = df.loc[(df["mail"] != "") | (df["mail"].isna())].index()
    df = df.loc[(df["mail"] != "") & (~df["mail"].isna())]

    # Gérer les références
    ref_cols = ["id_region", "id_direction"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_correspondant_profil(df: pd.DataFrame) -> pd.DataFrame:
    # Gérer les colonnes
    cols_to_keep = ["id", "type_de_correspondant"]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "id": "id_correspondant",
            "type_de_correspondant": "id_type_de_correspondant",
        }
    )
    df["id"] = (
        df.sort_values(by=["id_correspondant", "id_type_de_correspondant"])
        .reset_index(drop=True)
        .index
    )

    # Gérer les références
    ref_cols = ["id_correspondant", "id_type_de_correspondant"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Convert str of list to python list
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_type_de_correspondant")
    df = df.explode(column="id_type_de_correspondant")

    return df


def process_correspondant_certification(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = ["mail", "type_de_correspondant"]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "promotion_fac": "id_promotion_fac",
        }
    )
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_effectif_dsci(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_laboratoires_territoriaux(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_quest_accompagnement_fac_hors_bercylab(
    df: pd.DataFrame,
) -> pd.DataFrame:
    return df


def process_quest_inscription_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_quest_satisfaction_accompagnement_cci(
    df: pd.DataFrame,
) -> pd.DataFrame:
    return df


def process_quest_satisfaction_formation_fac(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_quest_satisfaction_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_charge_agent_cci(df: pd.DataFrame) -> pd.DataFrame:
    return df
