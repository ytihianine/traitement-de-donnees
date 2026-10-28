import pandas as pd
from modules.generic_processing.number import convert_to_numeric
from modules.generic_processing.text import normalize_whitespace_columns


def process_nomenclature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the nomenclature dataset.
    """
    rename = {
        "Exercice": "exercice",
        "programme": "programme",
        "brique": "brique",
        "libellé brique": "libelle_brique",
        "activité": "activite",
        "libellé activité": "libelle_activite",
        "Domaine fonctionnel": "domaine_fonctionnel",
        "Libellé DF": "libelle_df",
        "BOP": "bop",
        "UO": "uo",
    }
    df = df.rename(columns=rename)

    # Additional processing
    df["brique"] = df["brique"].str.split().str[-1]

    # Generic processing
    txt_columns = [
        "brique",
        "libelle_brique",
        "activite",
        "libelle_activite",
        "domaine_fonctionnel",
        "libelle_df",
        "bop",
        "uo",
    ]
    df = normalize_whitespace_columns(df, columns=txt_columns)

    num_columns = ["exercice", "programme", "brique"]
    df = convert_to_numeric(df, columns=num_columns, errors="coerce")

    return df


def process_programmation_annuelle(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the programmation_annuelle dataset.
    """
    rename = {
        "Exercice": "exercice",
        "n°programme": "numero_programme",
        "BOP": "bop",
        "type": "type",
        "brique": "brique",
        "action": "action",
        "code": "code",
        "libellé": "libelle",
        "AE": "execution_annee_precedente_ae",
        "CP": "execution_annee_precedente_cp",
        "AE.1": "repartition_ressource_ae",
        "CP.1": "repartition_ressource_cp",
        "AE.2": "execution_ae",
        "CP.2": "execution_cp",
        "CP.3": "depense_obligatoire_cp",
        "AE.3": "depense_ineluctable_ae",
        "CP.4": "depense_ineluctable_cp",
        "CP.5": "depense_ineluctable_dont_rest_a_payer_cp",
        "AE.4": "autres_depenses_ae",
        "CP.6": "autres_depenses_cp",
        "AE.5": "total_depenses_programmees_ae",
        "CP.7": "total_depenses_programmees_cp",
        "Type dépense": "type_depense",
        "RDV Budgétaire": "rdv_budgetaire",
    }
    df = df.rename(columns=rename)

    # Additional processing
    df["brique"] = df["brique"].str.split().str[-1]

    # Generic processing
    txt_columns = [
        "bop",
        "type",
        "brique",
        "action",
        "code",
        "libelle",
        "type_depense",
        "rdv_budgetaire",
    ]
    df = normalize_whitespace_columns(df, columns=txt_columns)

    num_columns = [
        "exercice",
        "numero_programme",
        "brique",
        "execution_annee_precedente_ae",
        "execution_annee_precedente_cp",
        "repartition_ressource_ae",
        "repartition_ressource_cp",
        "execution_ae",
        "execution_cp",
        "depense_obligatoire_cp",
        "depense_ineluctable_ae",
        "depense_ineluctable_cp",
        "depense_ineluctable_dont_rest_a_payer_cp",
        "autres_depenses_ae",
        "autres_depenses_cp",
        "total_depenses_programmees_ae",
        "total_depenses_programmees_cp",
    ]
    df = convert_to_numeric(df, columns=num_columns, errors="coerce")

    return df


def process_execution_mensuelle(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the execution_mensuelle dataset.
    """
    rename = {
        "Exercice": "exercice",
        "Programme": "programme",
        "BOP": "bop",
        "UO": "uo",
        "n° activité": "numero_activite",
        "Libellé activité": "libelle_activite",
        "Titre": "titre",
        "Domaine fonctionnel": "domaine_fonctionnel",
        "Fonds": "fonds",
        "N° mois": "numero_mois",
        "Mois": "mois",
        "AE": "ae",
        "CP": "cp",
    }
    df = df.rename(columns=rename)

    txt_columns = ["bop", "uo", "numero_activite", "libelle_activite", "titre", "domaine_fonctionnel", "fonds", "mois"]
    df = normalize_whitespace_columns(df, columns=txt_columns)

    # Unpivot
    df = df.melt(
        id_vars=[col for col in rename.values() if col not in ["ae", "cp"]],
        value_vars=["ae", "cp"],
        var_name="type_montant",
        value_name="montant_execution",
    )

    # Remove leading 0
    df["domaine_fonctionnel"] = df["domaine_fonctionnel"].str.lstrip("0")

    # Add date columns using exercice, numero_mois and last day of the month
    df["date_execution"] = pd.to_datetime(
        df["exercice"].astype(str) + "-" + df["numero_mois"].astype(str) + "-01", format="%Y-%m-%d"
    ) + pd.offsets.MonthEnd(n=0)

    return df


def process_lfi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the lfi dataset.
    """
    rename = {
        "Exercice": "exercice",
        "Programme": "programme",
        "Libellé programme": "libelle_programme",
        "n° Brique": "brique",
        "Libellé Brique": "libelle_brique",
        "AE": "ae",
        "CP": "cp",
    }
    df = df.rename(columns=rename)

    # Unpivot
    df = df.melt(
        id_vars=[col for col in rename.values() if col not in ["ae", "cp"]],
        value_vars=["ae", "cp"],
        var_name="type_montant",
        value_name="montant_lfi",
    )

    # Remove leading 0
    df["brique"] = df["brique"].str.lstrip("B")

    # Generic processing
    txt_columns = ["programme", "libelle_programme", "brique", "libelle_brique", "type_montant"]
    df = normalize_whitespace_columns(df, columns=txt_columns)

    num_columns = ["exercice", "programme", "brique", "montant_lfi"]
    df = convert_to_numeric(df, columns=num_columns, errors="coerce")

    return df
