import os
import pathlib
from configparser import ConfigParser
from typing import Iterable, Sequence

from infra.http_client.config import ClientConfig
from infra.http_client.factory import create_http_client

from enums.http import HttpHandlerType


EVENT_TYPES = ["add", "update"]
TBL_TO_EXCLUDE = ("test", "onglet", "doc")


current_dir = pathlib.Path(__file__).parent.resolve()
config = ConfigParser()
config.read(filenames=os.path.join(current_dir, "vars.cfg"))


def filter_tables(
    tables: Iterable[str], tbl_to_exclude: Iterable[str] = TBL_TO_EXCLUDE
) -> Sequence[str]:
    return [
        table for table in tables if not table.lower().startswith(tuple(tbl_to_exclude))
    ]


if __name__ == "__main__":
    # External clients
    http_config = ClientConfig(
        user_agent=config["HTTP"]["AGENT"], proxy=config["HTTP"]["PROXY"]
    )
    http_client = create_http_client(
        client_type=HttpHandlerType.REQUEST, config=http_config
    )
    headers = {
        "Authorization": f"Bearer {config["GRIST"]["TOKEN"]}",
        "accept": "application/json",
    }

    # Récupérer toutes les tables du document
    response = http_client.get(
        endpoint=config["GRIST"]["GRIST_HOST"]
        + "/api/docs/"
        + config["GRIST"]["DOC_ID"]
        + "/tables",
        headers=headers,
    )
    tables = [table["id"] for table in response.json()["tables"]]
    print("Nombre de tables dans le document: ", len(tables))
    print(f"Liste des tables: {tables}")
    tables = filter_tables(tables=tables)
    print("Nombre de tables dans le document après filtrage: ", len(tables))
    print(f"Liste des tables: {tables}")

    # Créer les webhooks
    webhooks = []
    for table in tables:
        webhook_info = {
            "fields": {
                "name": table,
                "memo": "Auto generated",
                "url": config["GRIST"]["N8N_PIPELINE_URL"],
                "enabled": True,
                "eventTypes": EVENT_TYPES,
                "isReadyColumn": None,
                "tableId": table,
            }
        }
        webhooks.append(webhook_info)

    print("Exemple: \n", webhooks[0])

    http_client.post(
        endpoint=config["GRIST"]["GRIST_HOST"]
        + "/api/docs/"
        + config["GRIST"]["DOC_ID"]
        + "/webhooks",
        headers=headers | {"Content-Type": "application/json"},
        json={"webhooks": webhooks},
    )
