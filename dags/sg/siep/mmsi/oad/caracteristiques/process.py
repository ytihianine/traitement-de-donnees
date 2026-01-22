import pandas as pd
import numpy as np


def merge_old_df_to_new_df(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame,
    id_keys: list[str],
    old_cols_to_keep: list[str] | None = None,
) -> pd.DataFrame:
    """
    Certaines données sont disponibles via d'autres chaines de traitement.
    Il faut donc les conserver.
    """
    if old_cols_to_keep is None:
        old_cols_to_keep = list(old_df.columns)

    df = pd.merge(
        left=new_df, right=old_df.loc[:, old_cols_to_keep], on=id_keys, how="left"
    )

    return df


def process_oad_file(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[
        (df["presence_mef_bat"] == "Avec MEF") & (df["filtre_manuel_a_conserver"])
    ]
    df = df.replace("NC", pd.NA)
    df = df.replace("s/o", pd.NA)
    df["code_insee_normalise"] = df["code_insee_normalise"].astype(str)
    return df


def process_sites(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_site",
        "libelle_site",
        "site_mef_hmef",
    ]
    df = df.loc[:, cols_to_keep]

    df = (
        df.dropna(subset=["code_site"])
        .assign(libelle_site=df["libelle_site"].str.upper())
        .convert_dtypes()
    )

    df = df.drop_duplicates(subset=["code_site", "libelle_site"], ignore_index=True)

    return df


def process_biens(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "categorie_administrative_liste_bat",
        "categorie_administrative_principale_bat",
        "code_bat_ter",
        "code_site",
        "date_construction_annee_corrigee",
        "gest_princ_personnalite_juridique",
        "gest_princ_personnalite_juridique_precision",
        "gest_princ_personnalite_juridique_simplifiee",
        "gestion_categorie",
        "gestion_mono_multi_mef",
        "gestion_mono_multi_min",
        "gestionnaire_presents_liste_mef",
        "gestionnaire_principal_code",
        "gestionnaire_principal_libelle",
        "gestionnaire_principal_libelle_abrege",
        "gestionnaire_principal_libelle_simplifie",
        "gestionnaire_principal_lien_mef",
        "gestionnaire_principal_ministere",
        "gestionnaire_type_simplifie_bat",
        "groupe_autorisation",
        "libelle_bat_ter",
        "periode",
        "presence_mef_bat",
    ]
    df = df.loc[:, cols_to_keep]

    df = (
        df.dropna(subset=["code_bat_ter"])
        .assign(libelle_bat_ter=df["libelle_bat_ter"].str.upper())
        .convert_dtypes()
    )
    df = df.drop_duplicates(subset=["code_bat_ter"], keep="first", ignore_index=True)
    return df


def process_gestionnaires(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_gestionnaire",
        "libelle_abrege",
        "libelle_gestionnaire",
        "libelle_simplifie",
        "lien_mef_gestionnaire",
        "ministere",
        "personnalite_juridique",
        "personnalite_juridique_precision",
        "personnalite_juridique_simplifiee",
    ]
    df = df.loc[:, cols_to_keep]

    df = (
        df.dropna(subset=["code_gestionnaire"])
        .assign(
            libelle_gestionnaire=df["libelle_gestionnaire"]
            .str.upper()
            .str.split()
            .str.join(" ")
        )
        .convert_dtypes()
    )
    df = df.drop_duplicates(
        subset=["code_gestionnaire"], keep="first", ignore_index=True
    )

    return df


def process_biens_gestionnaires(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "code_gestionnaire",
        "indicateur_poste_gest_source",
        "indicateur_resident_gest_source",
        "indicateur_sub_gest_source",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.dropna(subset=["code_bat_ter", "code_gestionnaire"]).convert_dtypes()
    df = df.drop_duplicates(
        subset=["code_bat_ter", "code_gestionnaire"],
        keep="first",
        ignore_index=True,
    )
    # Create calculated columns
    df["code_bat_gestionnaire"] = (
        df[["code_bat_ter", "code_gestionnaire"]].astype(str).agg("_".join, axis=1)
    )
    return df


def process_biens_occupants(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "categorie_administrative",
        "categorie_administrative_simplifiee",
        "code_bat_ter",
        "code_gestionnaire",
        "coherence_erreur_possible_presence_occupant",
        "coherence_indicateur_resident_revu_occ",
        "coherence_indicateur_sub_revu_occ",
        "comprend_service_ac",
        "direction_locale_occupante",
        "direction_locale_occupante_principale",
        "filtre_spsi_initial",
        "filtre_spsi_maj",
        "indicateur_poste_occ",
        "indicateur_poste_occ_source",
        "indicateur_resident_occ",
        "indicateur_resident_occ_source",
        "indicateur_resident_reconstitue_occ",
        "indicateur_sub_occ",
        "indicateur_sub_occ_source",
        "indicateur_surface_mef_occ",
        "indicateur_surface_spsi_m_sub_occ_initial",
        "indicateur_surface_spsi_m_sub_occ_maj",
        "occupant",
        "service_occupant",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.dropna(subset=["code_bat_ter", "code_gestionnaire"]).convert_dtypes()

    # Create calculated columns
    df["code_bat_gestionnaire"] = (
        df[["code_bat_ter", "code_gestionnaire"]].astype(str).agg("_".join, axis=1)
    )

    # Reset index to ensure a clean row-based id (starting from 1)
    df = df.reset_index(drop=True)

    # Fill missing or empty occupant values with "NR - <row_number>"
    df["occupant"] = np.where(
        df["occupant"].isna() | (df["occupant"].astype(str).str.strip() == ""),
        "NR - " + (df.index + 1).astype(str),
        df["occupant"],
    )
    df["service_occupant"] = np.where(
        df["service_occupant"].isna()
        | (df["service_occupant"].astype(str).str.strip() == ""),
        "NR - " + (df.index + 1).astype(str),
        df["service_occupant"],
    )

    return df
