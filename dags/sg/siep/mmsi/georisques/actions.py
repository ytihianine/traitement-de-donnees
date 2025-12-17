from typing import Optional
import pandas as pd

from infra.database.factory import create_db_handler
from infra.http_client.adapters import AbstractHTTPClient, RequestsClient
from infra.http_client.config import ClientConfig
from utils.config.dag_params import get_db_info
from utils.config.vars import AGENT, PROXY, DEFAULT_PG_DATA_CONN_ID

from dags.sg.siep.mmsi.georisques.process import (
    format_query_param,
    format_risque_results,
)


def get_bien_from_db(context: dict) -> pd.DataFrame:
    # Hook & config
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    schema = get_db_info(context=context)["prod_schema"]
    snapshot_id = context["ti"].xcom_pull(
        key="snapshot_id", task_ids="get_projet_snapshot"
    )

    # Retrieve data
    df = db_handler.fetch_df(
        query=f"""SELECT sb.code_bat_ter, sbl.latitude, sbl.longitude, sbl.adresse_normalisee,
                sbl.import_timestamp as import_timestamp_oad
            FROM {schema}.bien sb
            JOIN {schema}.bien_localisation sbl
                ON sb.code_bat_ter = sbl.code_bat_ter
            WHERE
                sb.snapshot_id = %s
                AND sbl.import_timestamp = (
                    SELECT MAX(import_timestamp)
                    FROM siep.bien_localisation
                    WHERE snapshot_id = %s
            );
        """,
        parameters=(snapshot_id, snapshot_id),
    )

    return df


def get_risque(
    http_client: AbstractHTTPClient, url: str, query_param: Optional[str]
) -> dict[str, str]:
    result_json = {}

    if not query_param:
        result_json["statut"] = "Echec"
        result_json["statut_code"] = None
        result_json["raison"] = "Ce bien n'a pas de données de localisation"
        return result_json

    full_url = f"{url}?{query_param}"

    response = http_client.get(endpoint=full_url, timeout=180)

    if response.status_code == 200:
        result_json["statut"] = "Succès"
        result_json["statut_code"] = response.status_code
        result_json["raison"] = None
        result_json.update(response.json())
        return result_json
    else:
        result_json["statut"] = "Echec"
        result_json["statut_code"] = response.status_code
        result_json["raison"] = response.content
        return result_json


def get_georisques(df_bien: pd.DataFrame) -> pd.DataFrame:
    # Http client
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    http_internet_client = RequestsClient(config=http_config)

    # Get result from API
    api_host = "https://www.georisques.gouv.fr"
    api_endpoint = "api/v1/resultats_rapport_risque"
    url = "/".join([api_host, api_endpoint])

    risques_api_info = []
    risques_results = []
    nb_rows = len(df_bien)
    for row in df_bien.itertuples():
        print(f"{row.Index + 1}/{nb_rows}")
        query_param = format_query_param(
            adresse=row.adresse, latitude=row.latitude, longitude=row.longitude
        )
        risque_api_result = get_risque(
            http_client=http_internet_client, url=url, query_param=query_param
        )
        risque_api_result["code_bat_ter"] = row.code_bat_ter
        print(risque_api_result)
        formated_risques = format_risque_results(risques=risque_api_result)
        risques_api_info.append(
            {
                "code_bat_ter": risque_api_result["code_bat_ter"],
                "statut": risque_api_result["statut"],
                "statut_code": risque_api_result["statut_code"],
                "raison": risque_api_result["raison"],
            }
        )
        risques_results.extend(formated_risques)

    df = pd.DataFrame(data=risques_results)

    return df
