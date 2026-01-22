import requests
from typing import Any
from datetime import datetime


def get_weather_from_api() -> dict[str, Any] | None:
    print("Connexion API Open-Meteo...")

    url = "https://api.open-meteo.com/v1/forecast?latitude=48.85&longitude=2.35&current_weather=true"

    try:
        response = requests.get(url)
        data = response.json()

        current_weather = data.get("current_weather")

        return {
            "date": datetime.now().strftime("%Y-%m-%d"),  # today
            "city": "Paris",
            "weather": {
                "temps": current_weather.get("temperature"),
                "conditions": f"Wind speed: {current_weather.get('windspeed')} km/h",
            },
        }
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return None
