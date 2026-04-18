import pandas as pd
from airflow.sdk import Variable

from src.infra.http_client.adapters import HttpxClient, ClientConfig
from src.constants import AGENT, PROXY

from src.dags.sg.siep.mmsi.api_operat.config import BASE_URL, ID_STRUCTURES


# ================
# API Fonctions
# ================
def build_header() -> dict[str, str]:
    return {
        "Content-type": "application/json",
        "client_id": Variable.get(key="client_id_api_operat"),
        "client_secret": Variable.get(key="client_secret_api_operat"),
    }


def get_token(api_client: HttpxClient, url: str, id_structure_assujettie: str) -> str:
    endpoint = "/api/v1/operat/authentification"

    body = {
        "cleTiers": Variable.get(key="cle_tiers_api_operat"),
        "idStructureAssujettie": id_structure_assujettie,
        "cleUtilisateur": Variable.get(key="cle_utilisateur_api_operat"),
    }
    headers = build_header()

    result = api_client.post(endpoint=url + endpoint, json=body, headers=headers)

    return result.json()["token"]


def get_liste_declarations(api_client: HttpxClient, url: str, token: str) -> dict:
    endpoint = "/api/v1/operat/consommations"
    headers = build_header() | {"Authorization": f"Bearer {token}"}

    result = api_client.get(endpoint=url + endpoint, headers=headers)

    return result.json()


def get_consommation_by_id(
    api_client: HttpxClient, url: str, token: str, id_consommation: str
) -> dict:
    endpoint = "/api/v1/operat/consommation/"
    full_url = url + endpoint + id_consommation
    headers = build_header() | {"Authorization": f"Bearer {token}"}

    result = api_client.get(endpoint=full_url, headers=headers)
    return result.json()


# ================
# Fonctions de processing pour les tâches
# ================
def liste_declaration() -> list[dict]:
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = HttpxClient(config=client_config)

    # Main part
    api_result = []
    for idx, id_structure in enumerate(ID_STRUCTURES):
        print(
            f"({idx+1}/{len(ID_STRUCTURES)}) Récupération des déclarations pour la structure {id_structure}"
        )
        token = get_token(
            api_client=httpx_internet_client,
            url=BASE_URL,
            id_structure_assujettie=id_structure,
        )
        lst_declarations = get_liste_declarations(
            api_client=httpx_internet_client, url=BASE_URL, token=token
        )
        api_result.extend(lst_declarations.get("resultat", []))

    return api_result


def consommation_by_id(df_declarations: pd.DataFrame) -> list[dict]:
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = HttpxClient(config=client_config)

    # Main part
    api_result = []
    for idx, id_structure in enumerate(ID_STRUCTURES):
        print(
            f"({idx+1}/{len(ID_STRUCTURES)}) Récupération des déclarations pour la structure {id_structure}"
        )
        token = get_token(
            api_client=httpx_internet_client,
            url=BASE_URL,
            id_structure_assujettie=id_structure,
        )

        conso_ids = df_declarations["id_consommation"]
        for id_conso in conso_ids:
            detail_conso = get_consommation_by_id(
                api_client=httpx_internet_client,
                url=BASE_URL,
                token=token,
                id_consommation=str(id_conso),
            )
            api_result.extend(detail_conso)

    return api_result
