import logging
import pandas as pd

from utils.config.vars import NO_PROCESS_MSG
from utils.control.dates import convert_grist_date_to_date
from utils.control.structures import (
    convert_str_of_list_to_list,
    handle_grist_null_references,
)
from utils.control.text import normalize_whitespace_columns


"""
    Fonction de processing des référentiels
"""


def process_ref_certification(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_competence_particuliere(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_bureau(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_pole(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"bureau": "id_bureau"})
    return df


def process_ref_profil_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    txt_cols = ["profil_correspondant", "intitule_long", "created_by", "updated_by"]
    other_cols = ["id"]

    # Retirer les colonnes non-utiles
    cols_to_drop = list(set(df.columns) - set(txt_cols + other_cols))
    df = df.drop(columns=cols_to_drop)

    # Nettoyer les données
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_ref_promotion_fac(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_semainier(df: pd.DataFrame) -> pd.DataFrame:
    col_date = ["date_semaine"]
    df = convert_grist_date_to_date(df, col_date)
    df = df.drop(columns=["is_duplicate"])
    return df


def process_ref_qualite_service(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_date_debut_inactivite(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_region(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_type_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"pole": "id_pole"})

    # Gérer les références
    ref_cols = ["id_pole"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_ref_typologie_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    cols_a_nettoyer = ["typologie_accompagnement"]
    df = normalize_whitespace_columns(df, cols_a_nettoyer)
    return df


"""
    Fonction de processing des données Mission Innovation
"""


def process_accompagnement_mi(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = [
        "id",
        "intitule",
        "direction",
        "date_de_realisation",
        "statut",
        "pole",
        "type_d_accompagnement",
        "est_certifiant",
        "places_max",
        "nb_inscrits",
        "places_restantes",
        "est_ouvert_notation",
        "informations_complementaires",
        "id_type_accompagnement",
    ]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "direction": "id_direction",
            "pole": "id_pole",
            "type_d_accompagnement": "id_type_d_accompagnement",
        }
    )
    col_date = ["date_de_realisation"]
    df = convert_grist_date_to_date(df, columns=col_date)
    df = normalize_whitespace_columns(df=df, columns=["informations_complementaires"])
    # Gérer les références
    ref_cols = ["id_direction", "id_pole", "id_type_d_accompagnement"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_accompagnement_mi_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "accompagnement": "id_accompagnement",
            "type_d_accompagnement": "id_type_d_accompagnement",
        })
    num_cols = [
        "nombre_de_participants",
        "nombre_de_reponses",
        "taux_de_reponse",
        "note_moyenne_de_satisfaction"
        ]
    cols_presentes = [col for col in num_cols if col in df.columns]
    if cols_presentes:
        df[cols_presentes] = df[cols_presentes].apply(pd.to_numeric, errors="coerce")
    # Gestion des références
    ref_cols = ["id_accompagnement", "id_type_d_accompagnement"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_animateur_interne(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={"accompagnement": "id_accompagnement", "animateur": "id_animateur"}
    )
    ref_cols = ["id_accompagnement", "id_animateur"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


def process_animateur_externe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={"accompagnement": "id_accompagnement"})
    ref_cols = ["id_accompagnement"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_animateur_fac(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "accompagnement": "id_accompagnement",
            "animateur": "id_animateur",
            "certifications_souhaitees": "id_certifications_souhaitees",
            "certifications_validees": "id_certifications_validees"
            })
    # Gestion des refs multiples
    ref_cols_multiples = ["id_certifications_souhaitees", "id_certifications_validees"]
    for col in ref_cols_multiples:
        if col in df.columns:
            df = convert_str_of_list_to_list(df, col)
    # Gestion refs simple

    ref_cols = ["id_accompagnement", "id_animateur"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_laboratoires_territoriaux(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "direction": "id_direction",
        "region": "id_region"})
    ref_cols = ["id_direction", "id_region"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


def process_quest_inscription_pleniere(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "direction": "id_direction",
        "id_accompagnement": "id_id_accompagnement",
        "pleniere": "id_pleniere"
    })
    ref_cols = ["id_direction", "id_id_accompagnement", "id_pleniere"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_satisfaction_pleniere(df: pd.DataFrame) -> pd.DataFrame:
    df["note_globale"] = pd.to_numeric(df["note_globale"], errors="coerce").astype("Int64")
    # Gestion de doublons
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_inscription_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "direction": "id_direction",
        "region": "id_region",
        "passinnov": "id_passinnov",
        "id_accompagnement": "id_id_accompagnement"
        })
    # Gestion
    ref_cols = [
        "id_direction",
        "id_region",
        "id_passinnov",
        "id_id_accompagnement"
        ]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_satisfaction_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "quest_passinnov": "id_quest_passinnov",
        "id_passinnov": "id_id_passinnov"
    })
    # Gestion
    ref_cols = [
        "id_quest_passinnov",
        "id_id_passinnov"
        ]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_inscription_formation_codev(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "direction": "id_direction",
        "id_accompagnement": "id_id_accompagnement",
        "session_formation_codev": "id_session_formation_codev"
    })
    ref_cols = ["id_direction", "id_id_accompagnement", "id_session_formation_codev"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_satisfaction_formation_fac(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "quest_formation": "id_quest_formation",
        "promotion": "id_promotion",
        "id_formation": "id_id_formation"
    })
    cols_notes = ["note_module_1", "note_module_2", "note_module_3", "nps"]
    for col in cols_notes:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    # List
    df = convert_str_of_list_to_list(df, "envies_pour_la_suite")
    # Gestion des refs
    ref_cols = ["id_quest_formation", "id_promotion", "id_id_formation"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


def process_quest_accompagnement_fac_hors_bercylab(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "direction": "id_direction",
        "region": "id_region",
        "facilitateur_1": "id_facilitateur_1",
        "facilitateur_2": "id_facilitateur_2",
        "facilitateur_3": "id_facilitateur_3",
        "facilitateurs": "id_facilitateurs"
        })
    # Convert date
    date_cols = ["date_de_realisation"]
    df = convert_grist_date_to_date(df=df, columns=date_cols)
    # Gestion des refs multiples
    ref_cols_multiples = ["id_facilitateurs"]
    for col in ref_cols_multiples:
        if col in df.columns:
            df = convert_str_of_list_to_list(df, col)
    # Gestion refs simple

    ref_cols = [
        "id_direction",
        "id_region",
        "id_facilitateur_1",
        "id_facilitateur_2",
        "id_facilitateur_3"
        ]
    # Gestion formatage
    col_text = ["synthese_de_l_accompagnement"]
    df = normalize_whitespace_columns(df, col_text)
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


"""
    Fonction de processing des données Conseil Interne cci
"""


def process_accompagnement_opportunite_cci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"accompagnement": "id_accompagnement"})
    # securité pour les saisies manuelles sans passer par la reference
    df["id_accompagnement"] = pd.to_numeric(df["id_accompagnement"], errors="coerce")
    # Gestion des dates
    date_cols = [
        "date_de_reception",
        "date_de_proposition_d_accompagnement",
        "date_prise_de_decision"
    ]
    df = convert_grist_date_to_date(df=df, columns=date_cols)

    refs_cols = ["id_accompagnement"]
    df = handle_grist_null_references(df=df, columns=refs_cols)

    return df


def process_charge_agent_cci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "agent_e_": "id_agent_e_",
        "semaine": "id_semaine",
        "missions": "id_missions"}
        )
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")
    num_cols = ["temps_passe", "taux_de_charge"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    ref_cols = ["id_agent_e_", "id_semaine", "id_missions"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_quest_satisfaction_accompagnement_cci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "formulaire_accompagnement": "id_formulaire_accompagnement",
        "etape_de_cadrage": "id_etape_de_cadrage",
        "aide_methodologique": "id_aide_methodologique",
        "pilotage_et_suivi": "id_pilotage_et_suivi",
        "respect_calendrier": "id_respect_calendrier",
        "reactivite": "id_reactivite",
        "adaptabilite": "id_adaptabilite",
        "relationnel_client": "id_relationnel_client",
        "qualite_des_livrables": "id_qualite_des_livrables",
        "atteinte_objectifs": "id_atteinte_objectifs",
        "accompagnement": "id_accompagnement"
    })
    ref_cols = [
        "id_formulaire_accompagnement",
        "id_etape_de_cadrage",
        "id_aide_methodologique",
        "id_pilotage_et_suivi",
        "id_respect_calendrier",
        "id_reactivite",
        "id_adaptabilite",
        "id_relationnel_client",
        "id_qualite_des_livrables",
        "id_atteinte_objectifs",
        "id_accompagnement"
        ]
    df[ref_cols] = df[ref_cols].apply(pd.to_numeric, errors="coerce").astype("Int64")
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


"""
Fonction de processing dsci
"""


def process_effectif_dsci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={
        "bureau": "id_bureau",
        "pole": "id_pole"})
    # Gestion nettoyage
    cols_date = ["absent_depuis"]
    cols_a_controler = ["fonction"]
    df = convert_grist_date_to_date(df, columns=cols_date)
    df = normalize_whitespace_columns(df, columns=cols_a_controler)
    # Controle saisie
    choix_valides = ["responsable", "cheffe de service"]
    df.loc[~df["fonction"].isin(choix_valides), "fonction"] = pd.NA
    # Gestion doublon mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    # Gérer les références
    ref_cols = ["id_bureau", "id_pole"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


def process_accompagnement_dsci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"direction": "id_direction",
                            "typologie": "id_typologie",
                            "equipe_s_dsci": "id_equipe_s_dsci",
                            "porteur_dsci": "id_porteur_dsci",
                            "prestataire": "recours_prestataire"
                            })
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")
    date_cols = [
        "debut_previsionnel_de_l_accompagnement",
        "fin_previsionnelle_de_l_accompagnement",
        "date_de_cloture_questionnaire"
    ]
    col_text = ["commentaires_complements"]
    df = convert_grist_date_to_date(df, date_cols)
    df = normalize_whitespace_columns(df=df, columns=col_text)
    # Gérer les références multiples
    ref_cols_multiples = ["id_typologie", "id_equipe_s_dsci", "id_porteur_dsci"]
    for col in ref_cols_multiples:
        if col in df.columns:
            df = convert_str_of_list_to_list(df, col)
    # Gestion référence simple
    df = handle_grist_null_references(df=df, columns=["id_direction"])

    return df


"""
    Fonction de processing des données bilatérales
"""


def process_bilaterale(df: pd.DataFrame) -> pd.DataFrame:
    col_date = ["date_de_rencontre"]
    df = convert_grist_date_to_date(df, col_date)
    df = df.rename(columns={"direction": "id_direction"})
    return df


def process_bilaterale_remontee(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["int_direction"])
    df = df.rename(
        columns={
            "bilaterale": "id_bilaterale",
            "bureau": "id_bureau",
        }
    )
    df["information_a_remonter"] = (
        df["information_a_remonter"].str.strip().str.split().str.join(" ")
    )

    # Gérer les références
    ref_cols = ["id_bilaterale", "id_bureau"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


"""
    Fonction de processing des correspondants
"""


def process_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = [
        "id",
        "mail",
        "nom_complet",
        "direction",
        "entite",
        "region",
        "actif",
        "promotion_fac",
        "est_certifie_fac",
        "actif_communaute_fac",
        "direction_hors_mef",
        "prenom",
        "nom",
        "date_debut_inactivite",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "promotion_fac": "id_promotion_fac",
        }
    )

    # Convert date
    date_cols = ["date_debut_inactivite"]
    df = convert_grist_date_to_date(df=df, columns=date_cols)

    # Cleaning
    txt_cols = ["mail", "entite", "direction_hors_mef", "prenom", "nom", "nom_complet"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    df["entite"] = df["entite"].replace('"', "", regex=False)

    # Filtrer les lignes
    # rows_to_drop = df.loc[(df["mail"] != "") | (df["mail"].isna())].index()
    df = df.loc[(df["mail"] != "") & (~df["mail"].isna())]

    # Gérer les références
    ref_cols = ["id_region", "id_direction"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_correspondant_profil(df: pd.DataFrame) -> pd.DataFrame:
    # Gérer les colonnes
    cols_to_keep = ["id", "type_de_correspondant"]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "id": "id_correspondant",
            "type_de_correspondant": "id_type_de_correspondant",
        }
    )

    # Gérer les références
    ref_cols = ["id_correspondant", "id_type_de_correspondant"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Convert str of list to python list
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_type_de_correspondant")
    df = df.explode(column="id_type_de_correspondant")

    df["id"] = (
        df.sort_values(by=["id_correspondant", "id_type_de_correspondant"])
        .reset_index(drop=True)
        .index
    )

    return df


def process_correspondant_certification(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = ["mail", "type_de_correspondant"]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "promotion_fac": "id_promotion_fac",
        }
    )
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df