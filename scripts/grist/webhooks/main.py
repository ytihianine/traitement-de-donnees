from typing import Iterable, Sequence

from src.infra.http_client.config import ClientConfig
from src.infra.http_client.factory import create_http_client
from src.constants import custom_logger
from scripts.settings import get_settings

from src._enums.http import HttpHandlerType

EVENT_TYPES = ["add", "update"]
TBL_TO_EXCLUDE = ("test", "onglet", "doc")

settings = get_settings()


def filter_tables(
    tables: Iterable[str], tbl_to_exclude: Iterable[str] = TBL_TO_EXCLUDE
) -> Sequence[str]:
    return [
        table for table in tables if not table.lower().startswith(tuple(tbl_to_exclude))
    ]


if __name__ == "__main__":
    # External clients
    http_config = ClientConfig(
        user_agent=settings.http.agent, proxy=settings.http.proxy
    )
    http_client = create_http_client(
        client_type=HttpHandlerType.REQUEST, config=http_config
    )
    headers = {
        "Authorization": f"Bearer {settings.grist.token}",
        "accept": "application/json",
    }

    # Récupérer toutes les tables du document
    response = http_client.get(
        endpoint=settings.grist.host + "/api/docs/" + settings.grist.doc_id + "/tables",
        headers=headers,
    )
    tables = [table["id"] for table in response.json()["tables"]]
    custom_logger.info(msg=f"Nombre de tables dans le document: {len(tables)}")
    custom_logger.info(msg=f"Liste des tables: {tables}")
    tables = filter_tables(tables=tables)
    custom_logger.info(
        msg=f"Nombre de tables dans le document après filtrage: {len(tables)}"
    )
    custom_logger.info(msg=f"Liste des tables: {tables}")

    # Créer les webhooks
    webhooks = []
    for table in tables:
        webhook_info = {
            "fields": {
                "name": table,
                "memo": "Auto generated",
                "url": settings.grist.n8n_pipeline_url,
                "enabled": True,
                "eventTypes": EVENT_TYPES,
                "isReadyColumn": None,
                "tableId": table,
            }
        }
        webhooks.append(webhook_info)

    custom_logger.info(msg=f"Exemple: \n{webhooks[0]}")

    http_client.post(
        endpoint=settings.grist.host
        + "/api/docs/"
        + settings.grist.doc_id
        + "/webhooks",
        headers=headers | {"Content-Type": "application/json"},
        json={"webhooks": webhooks},
    )
