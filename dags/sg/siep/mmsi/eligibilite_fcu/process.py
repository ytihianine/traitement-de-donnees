import pandas as pd
import numpy as np

from infra.http_client.base import AbstractHTTPClient
from utils.control.text import normalize_whitespace_columns


def can_perform_api_call(lat: float, lon: float) -> bool:
    """_summary_

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        bool: True if both lat and lon are defined
    """
    if pd.isna(lat) or pd.isna(lon):
        return False

    if not isinstance(lat, float) or not isinstance(lon, float):
        return False

    return True


def process_result(df_fcu: pd.DataFrame) -> pd.DataFrame:
    cols_mapping = {
        "isEligible": "is_eligible",
        "distance": "distance",
        "inPDP": "in_pdp",
        "isBasedOnIris": "is_based_on_iris",
        "futurNetwork": "futur_network",
        "id": "id_fcu",
        "name": "name",
        "gestionnaire": "gestionnaire_fcu",
        "rateENRR": "taux_enrr_rcu",
        "rateCO2": "rate_co2",
    }

    df = df_fcu.rename(columns=cols_mapping)
    txt_cols = ["name", "gestionnaire_fcu"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Colonnes additionnelles
    df["pdp"] = np.where(df["in_pdp"], "PDP", "HORS PDP")
    df["rcu_etat"] = np.where(df["futur_network"], "En construction", "En service")
    df["contenu_rcu_gco2"] = df["rate_co2"] * 1000

    palier = [0, 50, 100, 150, np.inf]
    labels = ["RCU à 50m", "RCU à 100m", "RCU à 150m", "SUP 150m"]
    df["distance_rattachement"] = pd.cut(
        x=df["distance"],
        bins=palier,
        labels=labels,
        right=True,
        include_lowest=False,
    )

    return df.convert_dtypes()


def get_eligibilite_fcu(
    api_client: AbstractHTTPClient, url: str, latitude: float, longitude: float
) -> dict[str, str]:
    result_json = {
        "api_status": None,
        "api_status_code": None,
        "api_raison": None,
    }

    if not can_perform_api_call(lat=latitude, lon=longitude):
        result_json["api_status"] = "Echec"
        result_json["api_raison"] = "Missing or invalid latitude/longitude"
        return result_json

    full_url = url + f"?lat={latitude}&lon={longitude}"
    response = api_client.get(full_url)

    if response.status_code == 200:
        result_json["api_status"] = "Succès"
        result_json["api_status_code"] = response.status_code
        result_json["api_raison"] = None
        result_json.update(response.json())
        return result_json
    else:
        result_json["api_status"] = "Echec"
        result_json["api_status_code"] = response.status_code
        result_json["api_raison"] = response.text
        return result_json
