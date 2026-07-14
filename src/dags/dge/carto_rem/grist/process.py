import pandas as pd


# ===========================================
# Functions de processing des Grist source
# ===========================================
def process_agent_revalorisation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[df["matricule_agent"] != 0]
    return df


def process_agent_contrat_complement(df: pd.DataFrame) -> pd.DataFrame:
    # Handle null byte values
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.replace("\x00", "", regex=False)

    return df


def process_agent_remuneration_complement(df: pd.DataFrame) -> pd.DataFrame:
    # Handle boolean columns
    df["present_cartographie"] = df["present_cartographie"].map({b"T": True, b"F": False, None: None})  # type: ignore

    return df


def process_agent_experience_pro(df: pd.DataFrame) -> pd.DataFrame:
    # Convertir les mois en valeurs décimales
    df["exp_pro_totale_mois"] = (df["exp_pro_totale_mois"] / 12).round(1)
    df["exp_qualifiante_sur_le_poste_mois"] = (df["exp_qualifiante_sur_le_poste_mois"] / 12).round(1)

    # Calculer l'exp pro en année
    df["experience_pro_totale"] = df.loc[:, "exp_pro_totale_annee"] + df.loc[:, "exp_pro_totale_mois"]
    df["experience_pro_qualifiante_sur_poste"] = (
        df.loc[:, "exp_qualifiante_sur_le_poste_annee"] + df.loc[:, "exp_qualifiante_sur_le_poste_mois"]
    )

    df = df.loc[df["matricule_agent"] != 0]

    return df
