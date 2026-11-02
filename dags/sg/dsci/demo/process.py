import pandas as pd
from modules.infra.airflow.common_tasks.grist import generic_grist_processing


# =============================================================
# Fonction de processing des référentiels
# =============================================================
def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        txt_columns=["direction"],
    )
    return df


def process_ref_type_demande(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        txt_columns=["type_demande"],
    )
    return df


# =============================================================
# Fonction de processing de la structure des données
# =============================================================
def process_equipe(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        txt_columns=["agent"],
    )
    return df


def process_demande(df: pd.DataFrame) -> pd.DataFrame:
    df = generic_grist_processing(
        df=df,
        cols_mapping={
            "direction": "id_direction",
            "type_demande": "id_type_demande",
            "assigne_a": "id_agent",
            "cree_le": "date_creation",
            "derniere_mise_a_jour": "date_modification",
        },
        txt_columns=["objet", "description", "statut", "niveau_de_complexite"],
        ref_columns=["id_direction", "id_type_demande", "id_agent"],
        date_columns=["date_creation", "date_modification"],
    )
    return df
