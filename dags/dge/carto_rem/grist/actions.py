from typing import Any
import pandas as pd

from infra.database.factory import create_db_handler
from utils.config.dag_params import get_db_info
from utils.config.vars import DEFAULT_PG_DATA_CONN_ID


def get_agent_contrat(context: dict[str, Any]) -> pd.DataFrame:
    schema = get_db_info(context=context)["prod_schema"]

    # Hook
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Retrieve data
    df = db_handler.fetch_df(
        query=f"""
            SELECT matricule_agent, mois_analyse, date_debut_contrat_actuel,
                date_fin_contrat_previsionnelle_actuel
            FROM {schema}.agent_contrat;
        """
    )

    return df
