import time
from typing import Optional
from infra.http_client.types import HTTPResponse
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
    http_client, url: str, query_param: str, max_retries: int = 3, retry_delay: int = 15
) -> Optional[HTTPResponse]:
    """
    Effectue une requête avec retry en cas d'erreur.

    Args:
        http_client: Client HTTP
        url: URL de l'API
        query_param: Paramètres de la requête
        max_retries: Nombre maximum de tentatives
        retry_delay: Délai d'attente entre chaque tentative (en secondes)

    Returns:
        HTTPResponse ou None en cas d'échec après tous les retries
    """
    retry_status_codes = {429, 500, 502, 503, 504}  # Codes d'erreur à retry

    for attempt in range(max_retries):
        try:
            full_url = f"{url}?{query_param}"
            response = response = http_client.get(endpoint=full_url, timeout=180)

            # Si succès, retourner la réponse
            if response and response.status_code == 200:
                return response

            # Si erreur à retry et pas la dernière tentative
            if (
                response
                and response.status_code in retry_status_codes
                and attempt < max_retries - 1
            ):
                wait_time = retry_delay * (attempt + 1)  # Backoff exponentiel optionnel
                print(
                    f"⚠️ Erreur {response.status_code}, nouvelle tentative dans {wait_time}s... ({attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                continue

            # Sinon retourner la réponse (même en erreur)
            return response

        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (attempt + 1)
                print(
                    f"⚠️ Exception: {str(e)}, nouvelle tentative dans {wait_time}s... ({attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
            else:
                print(f"❌ Échec après {max_retries} tentatives: {str(e)}")
                return None

    return None


def get_georisques(df_bien: pd.DataFrame) -> pd.DataFrame:
    # Http client
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    http_internet_client = RequestsClient(config=http_config)

    # Get result from API
    api_host = "https://www.georisques.gouv.fr"
    api_endpoint = "api/v1/resultats_rapport_risque"
    url = "/".join([api_host, api_endpoint])

    risques_results = []
    nb_rows = len(df_bien)
    for row in df_bien.itertuples():
        print(f"{row.Index + 1}/{nb_rows}")
        query_param = format_query_param(
            adresse=row.adresse_normalisee,
            latitude=row.latitude,
            longitude=row.longitude,
        )

        api_response = None
        if query_param:
            api_response = get_risque(
                http_client=http_internet_client, url=url, query_param=query_param
            )

        formated_risques = format_risque_results(
            code_bat_ter=row.code_bat_ter, api_response=api_response
        )
        print(formated_risques)
        risques_results.extend(formated_risques)

    df = pd.DataFrame(data=risques_results)

    return df
