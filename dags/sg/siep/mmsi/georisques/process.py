from typing import Any, Optional

from infra.http_client.types import HTTPResponse
import pandas as pd


def format_risque_results(
    code_bat_ter: int, api_response: Optional[HTTPResponse]
) -> list[dict[str, Any]]:
    # Cas d'erreur : pas de réponse API
    if api_response is None:
        return [
            {
                "code_bat_ter": code_bat_ter,
                "statut": "Echec",
                "statut_code": None,
                "raison": "Ce bien n'a pas de données de localisation",
            }
        ]

    # Cas d'erreur : réponse API avec erreur
    if api_response.status_code != 200:
        return [
            {
                "code_bat_ter": code_bat_ter,
                "statut": "Echec",
                "statut_code": api_response.status_code,
                "raison": api_response.content,
            }
        ]

    # Cas de succès : formater les risques
    risques = api_response.json()

    # Métadonnées communes à tous les risques
    metadata = {
        "code_bat_ter": code_bat_ter,
        "statut": "Succès",
        "statut_code": api_response.status_code,
        "raison": None,
    }

    formated_risques = []

    # Traiter les risques naturels
    for risque_type, risque_data in risques.get("risquesNaturels", {}).items():
        formated_risques.append(
            {
                **metadata,
                "risque_categorie": "Naturel",
                "risque_present": risque_data.get("present"),
                "risque_libelle": risque_data.get("libelle"),
            }
        )

    # Traiter les risques technologiques
    for risque_type, risque_data in risques.get("risquesTechnologiques", {}).items():
        formated_risques.append(
            {
                **metadata,
                "risque_categorie": "Technologique",
                "risque_present": risque_data.get("present"),
                "risque_libelle": risque_data.get("libelle"),
            }
        )

    return formated_risques


def format_query_param(
    adresse: str, latitude: float, longitude: float
) -> Optional[str]:
    if pd.isna(latitude) or pd.isna(longitude) or pd.isna(adresse):
        return None

    if isinstance(latitude, float) and isinstance(longitude, float):
        return f"latlon={longitude},{latitude}"

    if adresse:
        return f"adresse={adresse}"

    return None
