from functools import partial
from datetime import datetime
import pandas as pd
import numpy as np

from src.utils.process.text import (
    normalize_whitespace_columns,
    convert_str_cols_to_date,
)

mapping_direction = {
    "ASSOCIATION": {
        "AAF": "AAF",
        "AEB": "AEB",
        "AGRAF": "AGRAF",
        "ALPAF": "ALPAF",
        "ANC COMB": "ANC COMB",
        "APAHF": "APAHF",
        "ARTS": "ARTS",
        "ATSCAF": "ATSCAF",
        "CLUB SPORT": "CLUB SPORTIF",
        "EPAF": "EPAF",
        "ASFV": "ASFV",
        "ASFL": "ASFL",
        "ASFR": "ASFR",
        "FASF": "FASF",
        "FEMMES BER": "FEMMES BER",
    },
    "AUTORITES": {
        "ANAFE": "ANAFE",
        "AC": "AC",
        "ACPR": "ACPR",
        "ANC": "ANC",
        "ANJ": "ANJ",
        "ASN": "ASN",
        "APCR": "APCR",
        "ARCEP": "ARCEP",
        "CPT": "CPT",
        "CIF": "CIF",
        "CRE": "CRE",
    },
    "CONSEIL": {
        "CHAI": "CHAI",
        "CICAI": "CICAI",
        "CIE": "CIE",
        "CNC": "CNC",
        "CNN": "CNN",
        "CNOCP": "CNOCP",
    },
    "COMMISSIONS": {
        "ANAFE": "ANAFE",
        "CEPC": "CEPC",
        "CICC": "ANAFE",
        "CIF": "CIF",
        "CCLP": "CCLP",
        "CCCOP": "CCCOP",
        "CNS": "CNS",
        "CSNP": "CCSNP",
    },
    "MISSIONS": {
        "AMLA": "AMLA",
        "BER": "BER",
        "DIAMMS": "DIAMMS",
        "GIP-GEN": "GIP-GEN",
        "MAI": "MISSION-MAI",
        "MEA": "MISSION-MEA",
        "MGE": "MISSION-MGE",
        "ML": "ML",
        "MRP": "MRP",
        "RDM": "RDM",
        "OFGL": "OFGL",
        "PRO": "MISSION-PRO",
        "DTI": "DTI",
        "SIMP": "SIMP",
        "PRODVAC": "PRODVAC",
    },
    "DIRECTIONS ET SERVICES": {
        "AFT": "AFT",
        "APIE": "APIE",
        "AFA": "AFA",
        "AIFE": "AIFE",
        "APE": "APE",
        "CBCM": "CBCM",
        "CGE": "CGE",
        "CGEFI": "CGEFI",
        "CGEIET": "CGE",
        "CISIRH": "CISIRH",
        "DAE": "DAE",
        "DAJ": "DAJ",
        "DB": "DB",
        "DGAFP": "DGAFP",
        "DGE": "DGE",
        "DINSIC": "DINSIC",
        "DINUM": "DINUM",
        "DIRE": "DIRE",
        "DITP": "DITP",
        "DNLF": "DNLF",
        "IGF": "IGF",
        "MEDIASUP": "MEDIASUP",
        "MICAF": "MICAF",
        "SGAE": "SGAE",
        "TRACFIN": "TRACFIN",
        "DG TRESOR": "DG TRESOR",
        "BC": "BC",
        "EXT": "EXT",
    },
    "DIVERS": {
        "COOP": "COOP",
        "INC": "INC",
    },
    "ETABLISSEMENTS PUBLICS": {
        "ANFR": "ANFR",
        "CADES": "CADES",
        "AFII": "AFII",
        "GENES": "GENES",
        "IMT": "IMT",
    },
    "SEC GEN": {
        "BDS": "SG-BDS",
        "CAB": "SG-CAB",
        "DES": "SG-DES",
        "DSCI": "SG-DSCI",
        "DSI": "SG-DSI",
        "HFTLF": "SG-HFTLF",
        "IGPDE": "SG-IGPDE",
        "MFR": "SG-MFR",
        "MRC": "SG-MRC",
        "SAFI": "SG-SAFI",
        "SHFDS": "SG-SHFDS",
        "SEP2": "SG-SIEP",
        "SIEP": "SG-SIEP",
        "SIRCOM": "SG-SIRCOM",
        "SEP1": "SG-SNUM",
        "SNUM": "SG-SNUM",
        "SRH1/": "SG-SRH1",
        "SRH2/": "SG-SRH2",
        "SRH3/": "SG-SRH3",
        "SRH": "SG-SRH",
        "SEC GEN": "SG-AUTRE",
    },
    "MEDIATEUR": "MEDIATEUR",
    "MINISTRES": "CABINETS",
    "DGTRESOR": "DG TRESOR",
    "SYNDICATS": "SYNDICATS",
    "AUT MIN": "Autres ministères",
}


def get_direction_fom_nom_unite_geree(
    nom_unite_geree: str, map_direction: dict = mapping_direction
) -> str | None:
    """
    Déterminer la direction que gère l'AIP à partir du nom de l'unité gérée.
    Exemple de nom d'unité gérée :
        "struct_A/struct_B/ASSOCIATION" -> direction = "struct_B"
        "struc_D/struct_E/SEC GEN/SEC GEN" -> direction = "SG-struct_E"
        "struc_f/struct_G/SEC GEN/SEC GEN/Centrale" -> direction = "SG-struct_G"
    """
    direction = None
    if pd.isna(nom_unite_geree) or nom_unite_geree == "":
        return direction

    # Cas des antennes DGT
    if nom_unite_geree.startswith("DGTRESOR"):
        return "DG TRESOR"

    nom_unite_geree_split = nom_unite_geree.split(sep="/")

    # Retirer le suffixe "centrale" qui ne correspond pas à une direction
    if nom_unite_geree_split[-1].lower() == "centrale":
        nom_unite_geree_split = nom_unite_geree_split[:-1]

    type_structure = map_direction.get(nom_unite_geree_split[-1], None)

    if isinstance(type_structure, str):
        direction = type_structure

    if isinstance(type_structure, dict):
        for k, v in type_structure.items():
            if k in nom_unite_geree_split:
                direction = v

    return direction


def find_certificat_dir_in_profile(profile: str) -> str | None:
    direction = None

    if pd.isna(profile):
        return direction

    profile = profile.upper()
    mapping = {
        # Autres structures
        "PRESTAT": "ARTEMIS",
        "DGCCRF": "DGCCRF",
        "AIFE": "AIFE",
    }

    for key, value in mapping.items():
        if key in profile:
            return value

    return direction


def find_certificat_dir_in_dn(dn: str) -> str | None:
    direction = None

    if pd.isna(dn):
        return direction

    dn = dn.upper()
    mapping = {
        # Autres structures
        "DGFIP": "DGFIP",
        "SIRHIUS": "CISIRH",
        "AIFE": "AIFE",
        "OPENTRUST": "IGC",
        # SG
        "ALIZE": "SG_SNUM",
    }

    for key, value in mapping.items():
        if key in dn:
            direction = value

    return direction


def find_certificat_dir_in_subjectid(subjectid: str) -> str | None:
    direction = None

    if pd.isna(subjectid):
        return direction

    subjectid = subjectid.upper()
    mapping = {
        # Autres structures
        "TRACFIN": "TRACFIN",
        # SG
        "ARCADE": "SG_SNUM",
        "TEST": "SG_SNUM",
    }

    for key, value in mapping.items():
        if key in subjectid:
            direction = value
            return direction

    return direction


def find_certificat_dir_in_contact(contact: str) -> str | None:
    direction = None

    if pd.isna(contact):
        return direction

    contact = contact.upper()
    mapping = {
        # Autres structures
        "SIUDA": "DB",
        "DGFIP": "DGFIP",
        "DOUANE": "DGDDI",
        "AIFE": "AIFE",
        "INSEE": "INSEE",
        "CISIRH": "CISIRH",
        "CARGOET": "CISIRH",
        "LE-QUELLEC": "CISIRH",
        "CCRF": "DGCCRF",
        "@AFT": "AFT",
        # SG
        "SAFI": "SG_SAFI",
        "SEP": "SG_SNUM",
        "BEAUVOIS": "SG_SNUM",
        "BRUAL": "SG_SNUM",
        "NIANG": "SG_SNUM",
        "DJOUNNADI": "SG_SNUM",
        "SPOT": "SG_SNUM",
        "SDNAC": "SG_SNUM",
        "SHFDS": "SG_SHFDS",
    }

    for key, value in mapping.items():
        if key in contact:
            direction = value
            return direction

    return direction


def find_certificat_dir_in_mail(mail: str) -> str | None:
    if pd.isna(mail):
        return None
    mail = mail.upper()
    mapping = {
        # Autres structures
        "DGFIP": "DGFIP",
        "SGAE": "SGAE",
        "IGF": "IGF",
        "SYNDICATS": "SYNDICATS",
        "INDUSTRIE.GOUV": "CABINETS",
        "TRANSFORMATION.GOUV": "CABINETS",
        "NUMERIQUE.GOUV": "CABINETS",
        "INSEE": "INSEE",
    }

    for key, value in mapping.items():
        if key in mail:
            return value

    return None


def determiner_certificat_direction(df: pd.DataFrame) -> pd.DataFrame:
    """
    Déterminer à quelle direction le certificat est rattaché
    """
    # Déterminer la direction pour chaque informations disponibles
    # A partir du profil
    df["certif_dir_profile"] = list(map(find_certificat_dir_in_profile, df["profile"]))
    # A partir du DN
    df["certif_dir_dn"] = list(map(find_certificat_dir_in_dn, df["dn"]))
    # A partir du subjectid
    df["certif_dir_subjectid"] = list(
        map(find_certificat_dir_in_subjectid, df["subjectid"])
    )
    # A partir du contact
    df["certif_dir_contact"] = list(map(find_certificat_dir_in_contact, df["contact"]))
    # A partir du mail
    df["certif_dir_mail"] = list(map(find_certificat_dir_in_mail, df["email"]))

    # Déterminer la direction du certificat
    cols_sorted_by_priority = [
        "certif_dir_profile",
        "certif_dir_dn",
        "certif_dir_subjectid",
        "certif_dir_contact",
        "certif_dir_mail",
    ]
    df["certificat_direction"] = df[cols_sorted_by_priority].bfill(axis=1).iloc[:, 0]

    return df


def _match_mapping(
    text: str,
    mapping: dict[str, str] | dict[tuple[str, ...], str],
    default: str | None = None,
) -> str | None:
    """
    Matches `text` against keys in mapping.
    Keys can be a string (single condition) or a tuple (all substrings required).
    Returns the mapped value or `default`.
    """
    if text is None or pd.isna(text):
        return default

    text = text.upper()

    for keys, value in mapping.items():
        if isinstance(keys, str):  # single key
            keys = (keys,)
        if all(k.upper() in text for k in keys):
            return value
    return default


def determiner_ac(profile: str) -> str | None:
    mapping = {
        "SERVICE": "SERVICE",
        "SERVEUR": "SERVEURS",
        "AGENT": "AGENTS",
        "PRESTATAIRE": "PRESTATAIRES",
        "OPERATEURS": "OPERATEURS",
        "TEST": "TEST",
        "technique": "technique",
        "interne": "IGC",
        "AUTH_MOBILE": "MOBILE",
        "": "IGC",
    }

    return _match_mapping(text=profile, mapping=mapping)


def determiner_type_offre(profile: str) -> str | None:
    # Key order is important
    mapping = {
        # Autres structures
        "AUTH_SIGN": "AUTHENTIFICATION_SIGNATURE",
        "sign": "SIGNATURE",
        "auth": "AUTHENTIFICATION",
        "conf": "CONFIDENTIALITE",
        "prestataire": "ARTEMIS",
        "clients_multi": "CLIENT MULTIDOMAINE",
        "veurs_multi": "AUTHENTIFICATION MULTIDOMAINE",
        "horo": "HORODATAGE",
        "cachet": "CACHET",
        "serveurs_clien": "CLIENT SERVEUR",
        "operateur": "OPERATEUR",
        # Match exact values for those
        "F_SERVEURS": "AUTHENTIFICATION SERVEUR",
        # "F_SERVEURS_2": "AUTHENTIFICATION SERVEUR",
        # "F_SERVEURS_3": "AUTHENTIFICATION SERVEUR",
        # "serveurs": "",
    }
    return _match_mapping(text=profile, mapping=mapping)


def determiner_support(profile: str) -> str | None:
    # Key order is important
    mapping = {
        # Autres structures
        "TPM": "TPM",
        "logi": "LOGICIEL",
        "mate": "MATERIEL",
        "conf": "MATERIEL",
        "agents": "MATERIEL",
        "serveur": "SERVEUR",
        "service": "SERVICE",
        "ac_adm_technique": "MATERIEL",
    }
    return _match_mapping(text=profile, mapping=mapping)


def determiner_etat(
    date_debut_validite: datetime,
    date_fin_validite: datetime,
    date_revocation: datetime,
    date_ajd: datetime,
) -> str:
    # Check s'il y a une date de révocation
    if not pd.isna(date_revocation):
        return "REVOQUE"

    if date_ajd > date_fin_validite:
        return "EXPIRE"

    if date_debut_validite <= date_ajd and date_ajd <= date_fin_validite:
        return "VALIDE"

    return "Autre"


def determiner_version(profile: str) -> str | None:
    # Key order is important
    mapping = {
        ("SERVEURS", "_3"): "SERVEUR3",
        ("SERVICE", "_3"): "SERVICE3",
        ("CONFIDENTIALITE",): "CRYPT",
        ("FSG3_",): "AC3",
        ("SERVEURS",): "SERVEUR",
        ("service",): "SERVICE",
        ("FSG_",): "AC2",
    }
    return _match_mapping(text=profile, mapping=mapping)


def determiner_version_serveur(profile: str) -> str | None:
    # Key order is important
    mapping = {
        ("3072",): "3072",
        ("serveur", "_2"): "2048",
        ("service", "_2"): "2048",
        ("_3",): "2048",
    }
    return _match_mapping(text=profile, mapping=mapping)


def map_agent_direction(
    structure: str, mapping: dict = mapping_direction
) -> str | None:
    if pd.isna(structure):
        return None

    structure = structure.strip().upper()

    if structure.startswith("DGTRESOR"):
        return "DG TRESOR"

    structure_split = structure.split(sep="/")

    direction = mapping.get(structure_split[-1], None)

    if direction is None:
        return None

    if isinstance(direction, str):
        return direction.upper()

    if isinstance(direction, dict):
        for k, v in direction.items():
            if k in structure_split:
                return v

    return None


# ===============================================
# Fonctions de processing des fichiers sources
# ===============================================
def process_agent(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliser les données textuelles
    txt_cols = ["structure", "agent_mail"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Déterminer la direction de l'agent
    df["agent_direction"] = list(
        map(get_direction_fom_nom_unite_geree, df["structure"])
    )
    return df


def process_certificat(df: pd.DataFrame) -> pd.DataFrame:
    # df = df.fillna(np.nan).replace([np.nan], [None])
    date_cols = ["date_debut_validite", "date_fin_validite", "date_revocation"]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%Y-%m-%d %H:%M:%S", errors="raise"
    )

    # Ajout des colonnes additionnelles
    df = determiner_certificat_direction(df=df)
    date_ajd = datetime.now()
    df["ac"] = list(map(determiner_ac, df["profile"]))
    df["type_offre"] = list(map(determiner_type_offre, df["profile"]))
    df["supports"] = list(map(determiner_support, df["profile"]))
    df["etat"] = list(
        map(
            partial(determiner_etat, date_ajd=date_ajd),
            df["date_debut_validite"],
            df["date_fin_validite"],
            df["date_revocation"],
        )
    )
    df["version"] = list(map(determiner_version, df["profile"]))
    df["version_serveur"] = list(map(determiner_version_serveur, df["profile"]))
    return df


def process_aip(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliser les données textuelles
    txt_cols = ["structure", "aip_mail", "aip_balf_mail"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Déterminer la direction que gère l'AIP
    df["aip_direction_geree"] = list(
        map(get_direction_fom_nom_unite_geree, df["structure"])
    )

    return df


def process_historique_certificat(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliser les données textuelles
    txt_cols = ["agent_structure", "cn", "agent_mail"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Normaliser les dates
    date_cols = ["date_debut_validite", "date_fin_validite"]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%d.%m.%Y", errors="raise"
    )

    # Déterminer la direction de l'agent
    df["agent_direction"] = list(
        map(get_direction_fom_nom_unite_geree, df["agent_structure"])
    )

    return df


def process_mandataire(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliser les données textuelles
    txt_cols = ["structure", "sigle", "libelle", "mail"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    # Normaliser les dates
    date_cols = ["date"]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%d/%m/%Y", errors="coerce"
    )

    # Corriger les sigles
    df["sigle"] = df["sigle"].str.replace("/", "", regex=False)

    return df


# ===============================================
# Fonctions de processing des fichiers finaux
# ===============================================
def process_liste_certificat(
    df_certificat: pd.DataFrame, df_agent: pd.DataFrame
) -> pd.DataFrame:
    df_agents = df_agent[["agent_direction", "agent_mail"]].drop_duplicates()

    df_certificats_exp = df_certificat.copy().explode(
        column="contact", ignore_index=True
    )

    df = pd.merge(
        left=df_certificats_exp,
        right=df_agents,
        how="left",
        left_on=["contact"],
        right_on=["agent_mail"],
    )
    df["certificat_direction"] = np.where(
        df["certificat_direction"].isna(),
        df["agent_direction"],
        df["certificat_direction"],
    )

    # Conserver uniquement les colonnes nécessaires
    cols_to_keep = [
        "dn",
        "subjectid",
        "contact",
        "email",
        "date_debut_validite",
        "date_fin_validite",
        "profile",
        "status",
        "date_revocation",
        "certificat_direction",
        "ac",
        "type_offre",
        "supports",
        "etat",
        "version",
        "version_serveur",
    ]
    df = df.loc[:, cols_to_keep]

    return df
