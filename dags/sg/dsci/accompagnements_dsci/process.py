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
Fonctions de processing dsci
"""


def process_effectif_dsci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"bureau": "id_bureau", "pole": "id_pole"})
    cols_to_drop = [
        "created_at",
        "updated_at",
        "created_by",
        "updated_by",
        "bureau_texte",
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
    # Gestion nettoyage
    cols_date = ["absent_depuis"]
    df = convert_grist_date_to_date(df, columns=cols_date)
    # Gestion doublon mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    # Gérer les références
    ref_cols = ["id_bureau", "id_pole"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


def process_accompagnement_dsci(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = [
        "id",
        "annee",
        "direction",
        "service_bureau",
        "sous_dir_bureau_",
        "intitule_de_l_accompagnement",
        "statut",
        "prestataire",
        "nom_du_prestataire",
        "commentaires_complements",
        "ressources_documentaires",
        "debut_previsionnel_de_l_accompagnement",
        "fin_previsionnelle_de_l_accompagnement",
        "autres_participants",
        "date_de_cloture_questionnaire",
        "porteur_metier",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.rename(
        columns={"direction": "id_direction", "prestataire": "recours_prestataire"}
    )
    df["annee"] = pd.to_numeric(arg=df["annee"], errors="coerce").astype("Int64")  # type: ignore
    date_cols = [
        "debut_previsionnel_de_l_accompagnement",
        "fin_previsionnelle_de_l_accompagnement",
        "date_de_cloture_questionnaire",
    ]
    df = convert_grist_date_to_date(df, columns=date_cols)
    col_text = [
        "commentaires_complements",
        "intitule_de_l_accompagnement",
        "ressources_documentaires",
        "service_bureau",
        "sous_dir_bureau_",
        "porteur_metier",
    ]
    df = normalize_whitespace_columns(df=df, columns=col_text)
    # Gestion référence simple
    df = handle_grist_null_references(df=df, columns=["id_direction"])
    return df


def process_accompagnement_dsci_typologie(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "typologie"]
    df = df.loc[:, cols_to_keep]

    # Renommage
    df = df.rename(columns={"id": "id_accompagnement", "typologie": "id_typologie"})
    # Gestion des refs
    ref_cols = ["id_accompagnement", "id_typologie"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_typologie")
    df = df.explode(column="id_typologie")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_typologie"])
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_accompagnement", "id_typologie"]], index=False
    ) % (2**63)
    return df


def process_accompagnement_dsci_equipe(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "equipe_s_dsci"]
    df = df.loc[:, cols_to_keep]

    # Renommage
    df = df.rename(
        columns={"id": "id_accompagnement", "equipe_s_dsci": "id_equipe_s_dsci"}
    )
    # Gestion des refs
    ref_cols = ["id_accompagnement", "id_equipe_s_dsci"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_equipe_s_dsci")
    df = df.explode(column="id_equipe_s_dsci")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_equipe_s_dsci"])

    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_accompagnement", "id_equipe_s_dsci"]], index=False
    ) % (2**63)
    return df


def process_accompagnement_dsci_porteur(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "porteur_dsci"]
    df = df.loc[:, cols_to_keep]

    # Renommage
    df = df.rename(
        columns={"id": "id_accompagnement", "porteur_dsci": "id_porteur_dsci"}
    )
    # Gestion des refs
    ref_cols = ["id_accompagnement", "id_porteur_dsci"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_porteur_dsci")
    df = df.explode(column="id_porteur_dsci")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_porteur_dsci"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_accompagnement", "id_porteur_dsci"]], index=False
    ) % (2**63)
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
    ]
    df = df.loc[:, cols_to_keep]

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
        }
    )
    num_cols = [
        "nombre_de_participants",
        "nombre_de_reponses",
        "taux_de_reponse",
        "note_moyenne_de_satisfaction",
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
    df = df.rename(columns={"accompagnement": "id_accompagnement"})
    ref_cols = ["id_accompagnement"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_animateur_fac(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = [
        "id",
        "accompagnement",
        "animateur",
    ]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={"accompagnement": "id_accompagnement", "animateur": "id_animateur"}
    )

    # Gérer les références
    ref_cols = ["id_accompagnement", "id_animateur"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


def process_animateur_fac_certification(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "certifications_souhaitees"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={
            "id": "id_animateur_fac",
            "certifications_souhaitees": "id_certifications_souhaitees",
        }
    )
    # Gestion des refs
    ref_cols = ["id_animateur_fac", "id_certifications_souhaitees"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(
        df=df, col_to_convert="id_certifications_souhaitees"
    )
    df = df.explode(column="id_certifications_souhaitees")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_certifications_souhaitees"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_animateur_fac", "id_certifications_souhaitees"]], index=False
    ) % (2**63)
    return df


def process_animateur_fac_certification_valide(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "certifications_validees"]
    df = df.loc[:, cols_to_keep]
    # Renommage
    df = df.rename(
        columns={
            "id": "id_animateur_fac",
            "certifications_validees": "id_certifications_validees",
        }
    )
    # Gestion des refs
    ref_cols = ["id_animateur_fac", "id_certifications_validees"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_certifications_validees")
    df = df.explode(column="id_certifications_validees")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_certifications_validees"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_animateur_fac", "id_certifications_validees"]], index=False
    ) % (2**63)
    return df


def process_laboratoires_territoriaux(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"direction": "id_direction", "region": "id_region"})
    # Gestion des references
    ref_cols = ["id_direction", "id_region"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    return df


def process_quest_inscription_pleniere(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "direction": "id_direction",
            "id_accompagnement": "id_id_accompagnement",
            "pleniere": "id_pleniere",
        }
    )
    ref_cols = ["id_direction", "id_id_accompagnement", "id_pleniere"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_satisfaction_pleniere(df: pd.DataFrame) -> pd.DataFrame:
    df["note_globale"] = pd.to_numeric(df["note_globale"], errors="coerce").astype(  # type: ignore
        "Int64"
    )
    # Gestion de doublons
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_inscription_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "passinnov": "id_passinnov",
            "id_accompagnement": "id_id_accompagnement",
        }
    )
    # Gestion
    ref_cols = ["id_direction", "id_region", "id_passinnov", "id_id_accompagnement"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_satisfaction_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "quest_passinnov": "id_quest_passinnov",
            "id_passinnov": "id_id_passinnov",
        }
    )
    # Gestion
    ref_cols = ["id_quest_passinnov", "id_id_passinnov"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_inscription_formation_codev(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["is_duplicate"])
    df = df.rename(
        columns={
            "direction": "id_direction",
            "id_accompagnement": "id_id_accompagnement",
            "session_formation_codev": "id_session_formation_codev",
        }
    )
    col_text = [
        "formation_codev",
        "experience_codev",
        "details_experience",
        "difficultes",
        "attentes",
    ]
    df = normalize_whitespace_columns(df=df, columns=col_text)
    # Gestion des refs
    ref_cols = ["id_direction", "id_id_accompagnement", "id_session_formation_codev"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_quest_satisfaction_formation_fac(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = [
        "id",
        "quest_formation",
        "mail",
        "promotion",
        "note_module_1",
        "note_module_2",
        "note_module_3",
        "commentaire_m1",
        "commentaire_m2",
        "commentaire_m3",
        "nps",
        "utilite",
        "besoin",
        "id_formation",
    ]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    # Renommgae
    df = df.rename(
        columns={
            "quest_formation": "id_quest_formation",
            "promotion": "id_promotion",
            "id_formation": "id_id_formation",
        }
    )
    cols_notes = ["note_module_1", "note_module_2", "note_module_3", "nps"]
    for col in cols_notes:
        df[col] = pd.to_numeric(arg=df[col], errors="coerce").astype("Int64")  # type: ignore
    #  Nettoyage du texte
    col_text = [
        "commentaire_m1",
        "commentaire_m2",
        "commentaire_m3",
        "utilite",
        "besoin",
    ]
    df = normalize_whitespace_columns(df=df, columns=col_text)

    # Gestion des refs nulles
    ref_cols = ["id_quest_formation", "id_promotion", "id_id_formation"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Dédoublonnage sur le mail
    df = df.drop_duplicates(subset=["mail"], keep="last")

    return df


def process_quest_satisfaction_formation_fac_envies(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "envies_pour_la_suite"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(columns={"id": "id_formation_fac"})

    # Gestion des refs
    ref_cols = ["id_formation_fac"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="envies_pour_la_suite")
    df = df.explode(column="envies_pour_la_suite")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["envies_pour_la_suite"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_formation_fac", "envies_pour_la_suite"]], index=False
    ) % (2**63)

    return df


def process_quest_accompagnement_fac_hors_bercylab(df: pd.DataFrame) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = [
        "id",
        "intitule_de_l_accompagnement",
        "direction",
        "date_de_realisation",
        "statut",
        "synthese_de_l_accompagnement",
        "region",
        "facilitateur_1",
        "facilitateur_2",
        "facilitateur_3",
    ]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "facilitateur_1": "id_facilitateur_1",
            "facilitateur_2": "id_facilitateur_2",
            "facilitateur_3": "id_facilitateur_3",
            "facilitateurs": "id_facilitateurs",
        }
    )
    # Convert date
    date_cols = ["date_de_realisation"]
    df = convert_grist_date_to_date(df=df, columns=date_cols)
    # Gestion refs simple

    ref_cols = [
        "id_direction",
        "id_region",
        "id_facilitateur_1",
        "id_facilitateur_2",
        "id_facilitateur_3",
    ]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Gestion formatage
    col_text = ["synthese_de_l_accompagnement", "intitule_de_l_accompagnement"]
    df = normalize_whitespace_columns(df, columns=col_text)
    return df


def process_quest_accompagnement_fac_hors_bercylab_type(
    df: pd.DataFrame,
) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "type_d_accompagnement"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(columns={"id": "id_formation_fac_hors_bercylab"})

    # Gestion des refs
    ref_cols = ["id_formation_fac_hors_bercylab"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = convert_str_of_list_to_list(df=df, col_to_convert="type_d_accompagnement")
    df = df.explode(column="type_d_accompagnement")
    #  Nettoyage
    df = df.dropna(subset=["type_d_accompagnement"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_formation_fac_hors_bercylab", "type_d_accompagnement"]], index=False
    ) % (2**63)

    return df


def process_quest_accompagnement_fac_hors_bercylab_participants(
    df: pd.DataFrame,
) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "participants"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(columns={"id": "id_formation_fac_hors_bercylab"})

    # Gestion des refs
    ref_cols = ["id_formation_fac_hors_bercylab"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    df = convert_str_of_list_to_list(df=df, col_to_convert="participants")
    df = df.explode(column="participants")
    #  Nettoyage
    df = df.dropna(subset=["participants"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_formation_fac_hors_bercylab", "participants"]], index=False
    ) % (2**63)

    return df


def process_quest_accompagnement_fac_hors_bercylab_facilitateurs(
    df: pd.DataFrame,
) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "facilitateurs"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={
            "id": "id_formation_fac_hors_bercylab",
            "facilitateurs": "id_facilitateurs",
        }
    )
    # Gestion des refs
    ref_cols = ["id_formation_fac_hors_bercylab", "id_facilitateurs"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Converstion  str en list
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_facilitateurs")
    df = df.explode(column="id_facilitateurs")
    #  Nettoyage
    df = df.dropna(subset=["id_facilitateurs"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_formation_fac_hors_bercylab", "id_facilitateurs"]], index=False
    ) % (2**63)

    return df


"""
    Fonction de processing des données Conseil Interne cci
"""


def process_accompagnement_cci_opportunite(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"accompagnement": "id_accompagnement"})
    # securité pour les saisies manuelles sans passer par la reference
    df["id_accompagnement"] = pd.to_numeric(
        arg=df["id_accompagnement"], errors="coerce"
    )
    # Gestion des dates
    date_cols = [
        "date_de_reception",
        "date_de_proposition_d_accompagnement",
        "date_prise_de_decision",
    ]
    df = convert_grist_date_to_date(df=df, columns=date_cols)

    refs_cols = ["id_accompagnement"]
    df = handle_grist_null_references(df=df, columns=refs_cols)

    return df


def process_charge_agent_cci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "agent_e_": "id_agent_e_",
            "semaine": "id_semaine",
            "missions": "id_missions",
        }
    )
    df["annee"] = pd.to_numeric(arg=df["annee"], errors="coerce").astype("Int64")  # type: ignore
    num_cols = ["temps_passe", "taux_de_charge"]
    for col in num_cols:
        df[col] = pd.to_numeric(arg=df[col], errors="coerce")
    ref_cols = ["id_agent_e_", "id_semaine", "id_missions"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_quest_satisfaction_accompagnement_cci(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
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
            "accompagnement": "id_accompagnement",
        }
    )
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
        "id_accompagnement",
    ]
    df[ref_cols] = df[ref_cols].apply(pd.to_numeric, errors="coerce").astype("Int64")
    df = handle_grist_null_references(df=df, columns=ref_cols)
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
    ref_cols = ["id_region", "id_direction", "id_promotion_fac"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    return df


def process_correspondant_profil(df: pd.DataFrame) -> pd.DataFrame:
    # Gérer les colonnes
    cols_to_keep = ["id", "type_de_correspondant"]
    df = df.loc[:, cols_to_keep]
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
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_correspondant", "id_type_de_correspondant"]], index=False
    ) % (2**63)

    return df


def process_correspondant_competence_particuliere(df: pd.DataFrame) -> pd.DataFrame:
    # Gérer les colonnes
    cols_to_keep = ["id", "competence_particuliere"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(
        columns={
            "id": "id_correspondant",
            "competence_particuliere": "id_competence_particuliere",
        }
    )

    # Gérer les références
    ref_cols = ["id_correspondant", "id_competence_particuliere"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Convertion str en liste
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_competence_particuliere")
    df = df.explode(column="id_competence_particuliere")
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_correspondant", "id_competence_particuliere"]], index=False
    ) % (2**63)

    return df


def process_correspondant_connaissance_communaute(df: pd.DataFrame) -> pd.DataFrame:
    # Gérer les colonnes
    cols_to_keep = ["id", "connaissance_communaute"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(columns={"id": "id_correspondant"})
    # Gestion des references
    ref_cols = ["id_correspondant"]
    df = handle_grist_null_references(df=df, columns=ref_cols)
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="connaissance_communaute")
    df = df.explode(column="connaissance_communaute")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["connaissance_communaute"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_correspondant", "connaissance_communaute"]], index=False
    ) % (2**63)

    return df
