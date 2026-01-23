from typing import Any
from datetime import datetime

# Imports infra
from infra.http_client.factory import create_http_client
from infra.http_client.config import ClientConfig
from enums.dags import HttpHandlerType
#from dags.sg.dsci.weather_api.variables import PROXY, AGENT
from utils.config.vars import AGENT, PROXY


def get_weather_from_api() -> dict[str, Any]:
    """
    Récupère la météo via le Proxy.
    Config et Client
    """
    print("Initialisation du client HTTP...")

    # --- CONFIGURATION ---
    # args de ma class CltConfig
    config = ClientConfig(
        proxy=PROXY,
        user_agent=AGENT
    )

    # --- CRÉATION DU CLIENT ---
    # Type REQUESTS
    client = create_http_client(client_type=HttpHandlerType.REQUEST, config=config)

    # --- APPEL API ---
    print("Connexion API Open-Meteo...")
    url = "https://api.open-meteo.com/v1/forecast?latitude=48.85&longitude=2.35&current_weather=true"
    response = client.get(url)

    # --- Gestion Erreur --
    if not response.ok:
        raise Exception(f"Erreur API ({response.status_code}) : Impossible de récupérer la météo.")

    # --- TRAITEMENT DATA ---
    data = response.json()
    current_weather = data.get("current_weather")

    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "city": "Paris",
        "weather": {
            "temps": current_weather.get("temperature"),
            "conditions": f"Wind speed: {current_weather.get('windspeed')} km/h",
        },
    }