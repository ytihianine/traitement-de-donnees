import logging

import pandas as pd
from modules.constants import NO_PROCESS_MSG


# =============================================================
# Fonction de processing des référentiels
# =============================================================
def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_intervention(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# =============================================================
# Fonction de processing de la structure des données
# =============================================================


def process_agents(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df
