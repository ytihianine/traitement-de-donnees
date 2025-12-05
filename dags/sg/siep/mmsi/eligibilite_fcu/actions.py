from typing import Any
import pandas as pd

from infra.http_client.adapters import HttpxClient, ClientConfig, RequestsClient
from utils.config.vars import AGENT, DEFAULT_PG_DATA_CONN_ID, PROXY
from infra.database.factory import create_db_handler

from dags.sg.siep.mmsi.eligibilite_fcu.process import (
    get_eligibilite_fcu,
    process_result,
)
from utils.dataframe import df_info


def eligibilite_fcu(context: dict[str, Any]) -> pd.DataFrame:
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = RequestsClient(config=client_config)

    # Hooks
    db_hook = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Storage paths
    snapshot_id = context["ti"].xcom_pull(
        key="snapshot_id", task_ids="get_projet_snapshot"
    )
    df_oad = db_hook.fetch_df(
        query="""SELECT sbl.code_bat_ter, sbl.latitude, sbl.longitude, sbl.import_timestamp as import_timestamp_oad
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

    api_host = "https://france-chaleur-urbaine.beta.gouv.fr"
    api_endpoint = "api/v1/eligibility"
    url = "/".join([api_host, api_endpoint])

    api_results = []
    nb_rows = len(df_oad)
    for row in df_oad.itertuples():
        print(f"{row.Index}/{nb_rows}")
        api_result = get_eligibilite_fcu(
            api_client=httpx_internet_client,
            url=url,
            latitude=row.latitude,
            longitude=row.longitude,
        )
        api_result["code_bat_ter"] = row.code_bat_ter
        api_result["import_timestamp_oad"] = row.import_timestamp_oad
        api_results.append(api_result)
        print(api_result)

    df_result = pd.DataFrame(data=api_results)
    df_info(df=df_result, df_name="Result API - After processing")

    return df_result
