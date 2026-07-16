import logging
from typing import Any

import pandas as pd
from dags.sg.siep.mmsi.eligibilite_fcu.process import (
    get_eligibilite_fcu,
)
from modules.constants import AGENT, DEFAULT_PG_DATA_CONN_ID, PROXY
from modules.enums.http import HttpHandlerType
from modules.infra.database.factory import create_db_handler
from modules.infra.http_client.adapters import ClientConfig
from modules.infra.http_client.factory import create_http_client
from modules.utils.logs import df_info


def eligibilite_fcu(context: dict[str, Any]) -> pd.DataFrame:
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    http_internet_client = create_http_client(client_type=HttpHandlerType.REQUEST, config=client_config)

    # Hooks
    db_hook = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Storage paths
    snapshot_id = context["ti"].xcom_pull(key="return_value", task_ids="get_projet_snapshot")
    logging.info(msg=f"Snapshot ID récupéré : {snapshot_id}")
    df_oad = db_hook.fetch_df(
        query="""
        SELECT
            sbl.code_bat_ter,
            sbl.latitude,
            sbl.longitude,
            sbl.import_timestamp as import_timestamp_oad
        FROM siep.bien_localisation sbl
            WHERE sbl.snapshot_id = %s
            AND sbl.import_timestamp = (
                SELECT MAX(import_timestamp)
                FROM siep.bien_localisation
                WHERE snapshot_id = %s
            )
            ;
        """,
        parameters=(snapshot_id, snapshot_id),
    )
    if df_oad.empty:
        logging.warning(msg="Le DataFrame df_oad est vide. Fin du processus.")
        raise ValueError("Aucune données disponible dans df_oad.")

    api_host = "https://france-chaleur-urbaine.beta.gouv.fr"
    api_endpoint = "api/v1/eligibility"
    url = "/".join([api_host, api_endpoint])

    api_results = []
    nb_rows = len(df_oad)
    logging.info(msg=f"Nombre de bâtiments à traiter : {nb_rows}")
    for row in df_oad.itertuples():
        logging.info(msg=f"{row.Index}/{nb_rows}")
        api_result = get_eligibilite_fcu(
            api_client=http_internet_client,
            url=url,
            latitude=row.latitude,
            longitude=row.longitude,
        )
        api_result["code_bat_ter"] = row.code_bat_ter
        api_result["import_timestamp_oad"] = row.import_timestamp_oad
        api_results.append(api_result)
        logging.info(msg=api_result)

    df_result = pd.DataFrame(data=api_results)
    df_info(df=df_result, df_name="Result API - After processing")

    return df_result
