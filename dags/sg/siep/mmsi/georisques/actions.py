import logging
from typing import Optional

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry_if_result,
    before_sleep_log,
)
from infra.http_client.types import HTTPResponse
import pandas as pd

from infra.database.factory import create_db_handler
from infra.http_client.factory import create_http_client
from infra.http_client.config import ClientConfig
from utils.config.dag_params import get_db_info
from enums.http import HttpHandlerType
from utils.config.vars import AGENT, PROXY, DEFAULT_PG_DATA_CONN_ID

from dags.sg.siep.mmsi.georisques.process import (
    format_query_param,
    format_risque_results,
)


logger = logging.getLogger(name=__name__)


def get_bien_from_db(context: dict) -> pd.DataFrame:
    # Hook & config
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    schema = get_db_info(context=context).prod_schema
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


def _should_retry_response(response: Optional[HTTPResponse]) -> bool:
    """Determine if response should trigger a retry."""
    if response is None:
        return True
    retry_status_codes = {400, 404, 429, 500, 502, 503, 504}
    return response.status_code in retry_status_codes


@retry(
    stop=stop_after_attempt(max_attempt_number=3),
    wait=wait_exponential(multiplier=3, min=1, max=10),
    retry=(
        retry_if_exception_type(exception_types=Exception)
        | retry_if_result(predicate=_should_retry_response)
    ),
    before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    reraise=False,
)
def get_risque(http_client, url: str, query_param: str) -> Optional[HTTPResponse]:
    """
    Effectue une requête avec retry en cas d'erreur.

    Args:
        http_client: Client HTTP
        url: URL de l'API
        query_param: Paramètres de la requête

    Returns:
        HTTPResponse ou None en cas d'échec après tous les retries
    """
    full_url = f"{url}?{query_param}"
    response = http_client.get(endpoint=full_url, timeout=180)

    # Return response if successful (200) or non-retryable error
    if response and response.status_code == 200:
        return response

    # If response has retryable status code, trigger retry
    if response and response.status_code in {429, 500, 502, 503, 504}:
        logger.warning(msg=f"⚠️ Erreur {response.status_code}, nouvelle tentative...")
        raise Exception(f"Retryable status code: {response.status_code}")

    # For other errors, return response without retry
    return response


def get_georisques(df: pd.DataFrame) -> pd.DataFrame:
    # Http client
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    http_internet_client = create_http_client(
        client_type=HttpHandlerType.REQUEST, config=http_config
    )

    # Get result from API
    api_host = "https://www.georisques.gouv.fr"
    api_endpoint = "api/v1/resultats_rapport_risque"
    url = "/".join([api_host, api_endpoint])

    risques_results = []
    nb_rows = len(df)
    for row in df.itertuples():
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
