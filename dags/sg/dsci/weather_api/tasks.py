import pandas as pd
from airflow.sdk import task
from typing import Any

# Imports locaux
from dags.sg.dsci.weather_api import actions
from dags.sg.dsci.weather_api import process

# --- Imports & Config de YT ---


# 1. Tâche d'Extraction


@task()
def extract_data() -> dict[str, Any]:
    print("Extraction des données météo depuis l'API")
    return actions.get_weather_from_api()


# 2. Tâche de Transformation
@task()
def transform_data(raw_data: dict[str, Any]) -> list[list[Any]]:
    if not raw_data:
        print("Aucune donnée reçue !")
        return []
    weather_info = raw_data.get("weather", {})

    transformed_data = [
        [
            raw_data.get("date"),
            raw_data.get("city"),
            weather_info.get("temps"),
            weather_info.get("conditions"),
        ]
    ]
    return transformed_data


# 3. Tâche de Chargement
@task()
def load_data(transformed_data: list[list[Any]]) -> pd.DataFrame | None:
    # verif que liste pas vide
    if not transformed_data:
        print("Liste vide, rien à charger.")
        return

    # Appel 'process' : pour conversn
    loaded_data = process.convert_to_dataframe(transformed_data)
    loaded_data.columns = ["date", "city", "weather_temps", "weather_conditions"]
    print(loaded_data)
    return loaded_data
