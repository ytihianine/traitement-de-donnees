from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.accompagnements_dsci import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


def _grist_pipeline(dataset_name: str, custom_fn, **grist_kwargs) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=partial(
            generic_grist_processing,
            custom_fn=custom_fn,
            **grist_kwargs,
        ),
    )


@task_group
def referentiels() -> None:
    ref_bureau = create_task(
        pipeline=_grist_pipeline("ref_bureau", process.process_ref_bureau),
        execution_options=config.execution_options["ref_bureau"],
    )
    ref_certification = create_task(
        pipeline=_grist_pipeline("ref_certification", process.process_ref_certification),
        execution_options=config.execution_options["ref_certification"],
    )
    ref_competence_particuliere = create_task(
        pipeline=_grist_pipeline("ref_competence_particuliere", process.process_ref_competence_particuliere),
        execution_options=config.execution_options["ref_competence_particuliere"],
    )
    ref_direction = create_task(
        pipeline=_grist_pipeline("ref_direction", process.process_ref_direction),
        execution_options=config.execution_options["ref_direction"],
    )
    ref_profil_correspondant = create_task(
        pipeline=_grist_pipeline(
            "ref_profil_correspondant",
            process.process_ref_profil_correspondant,
            cols_to_keep=[
                "id",
                "profil_correspondant",
                "intitule_long",
                "created_by",
                "updated_by",
            ],
            txt_columns=[
                "profil_correspondant",
                "intitule_long",
                "created_by",
                "updated_by",
            ],
        ),
        execution_options=config.execution_options["ref_profil_correspondant"],
    )
    ref_qualite_service = create_task(
        pipeline=_grist_pipeline("ref_qualite_service", process.process_ref_qualite_service),
        execution_options=config.execution_options["ref_qualite_service"],
    )
    ref_region = create_task(
        pipeline=_grist_pipeline("ref_region", process.process_ref_region),
        execution_options=config.execution_options["ref_region"],
    )
    ref_semainier = create_task(
        pipeline=_grist_pipeline(
            "ref_semainier",
            process.process_ref_semainier,
            date_columns=["date_semaine"],
        ),
        execution_options=config.execution_options["ref_semainier"],
    )
    ref_typologie_accompagnement = create_task(
        pipeline=_grist_pipeline(
            "ref_typologie_accompagnement",
            process.process_ref_typologie_accompagnement,
            txt_columns=["typologie_accompagnement"],
        ),
        execution_options=config.execution_options["ref_typologie_accompagnement"],
    )
    ref_pole = create_task(
        pipeline=_grist_pipeline(
            "ref_pole",
            process.process_ref_pole,
            cols_mapping={"bureau": "id_bureau"},
        ),
        execution_options=config.execution_options["ref_pole"],
    )
    ref_type_accompagnement = create_task(
        pipeline=_grist_pipeline(
            "ref_type_accompagnement",
            process.process_ref_type_accompagnement,
            cols_mapping={"pole": "id_pole"},
            ref_columns=["id_pole"],
        ),
        execution_options=config.execution_options["ref_type_accompagnement"],
    )

    # Ordre des tâches
    chain(
        [
            ref_bureau(),
            ref_certification(),
            ref_competence_particuliere(),
            ref_direction(),
            ref_profil_correspondant(),
            ref_qualite_service(),
            ref_region(),
            ref_semainier(),
            ref_typologie_accompagnement(),
            ref_pole(),
            ref_type_accompagnement(),
        ],
    )


@task_group
def bilaterales() -> None:
    bilaterale = create_task(
        pipeline=_grist_pipeline(
            "bilaterale",
            process.process_bilaterale,
            cols_mapping={"direction": "id_direction"},
            ref_columns=["id_direction"],
            date_columns=["date_de_rencontre"],
        ),
        execution_options=config.execution_options["bilaterale"],
    )
    bilaterale_remontee = create_task(
        pipeline=_grist_pipeline(
            "bilaterale_remontee",
            process.process_bilaterale_remontee,
            cols_mapping={
                "bilaterale": "id_bilaterale",
                "bureau": "id_bureau",
            },
            txt_columns=["information_a_remonter"],
            ref_columns=["id_bilaterale", "id_bureau"],
        ),
        execution_options=config.execution_options["bilaterale_remontee"],
    )
    # Ordre des tâches
    chain([bilaterale(), bilaterale_remontee()])


@task_group
def correspondant() -> None:
    correspondant = create_task(
        pipeline=_grist_pipeline(
            "correspondant",
            process.process_correspondant,
            cols_to_keep=[
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
            ],
            cols_mapping={
                "direction": "id_direction",
                "region": "id_region",
                "promotion_fac": "id_promotion_fac",
            },
            date_columns=["date_debut_inactivite"],
            txt_columns=[
                "mail",
                "entite",
                "direction_hors_mef",
                "prenom",
                "nom",
                "nom_complet",
            ],
            ref_columns=["id_region", "id_direction", "id_promotion_fac"],
        ),
        execution_options=config.execution_options["correspondant"],
    )
    correspondant_profil = create_task(
        pipeline=_grist_pipeline(
            "correspondant_profil",
            process.process_correspondant_profil,
            cols_mapping={
                "id": "id_correspondant",
                "type_de_correspondant": "id_type_de_correspondant",
            },
            cols_to_keep=["id", "type_de_correspondant"],
            ref_columns=["id_correspondant"],
        ),
        execution_options=config.execution_options["correspondant_profil"],
    )
    correspondant_competence_particuliere = create_task(
        pipeline=_grist_pipeline(
            "correspondant_competence_particuliere",
            process.process_correspondant_competence_particuliere,
            cols_mapping={
                "id": "id_correspondant",
                "competence_particuliere": "id_competence_particuliere",
            },
            cols_to_keep=["id", "competence_particuliere"],
            ref_columns=["id_correspondant"],
        ),
        execution_options=config.execution_options["correspondant_competence_particuliere"],
    )
    correspondant_connaissance_communaute = create_task(
        pipeline=_grist_pipeline(
            "correspondant_connaissance_communaute",
            process.process_correspondant_connaissance_communaute,
            cols_mapping={"id": "id_correspondant"},
            cols_to_keep=["id", "connaissance_communaute"],
            ref_columns=["id_correspondant"],
        ),
        execution_options=config.execution_options["correspondant_connaissance_communaute"],
    )

    # Ordre des tâches
    chain(
        [
            correspondant(),
            correspondant_profil(),
            correspondant_competence_particuliere(),
            correspondant_connaissance_communaute(),
        ]
    )


@task_group
def dsci() -> None:
    accompagnement_dsci = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_dsci",
            process.process_accompagnement_dsci,
            cols_mapping={
                "direction": "id_direction",
                "prestataire": "recours_prestataire",
            },
            cols_to_keep=[
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
            ],
            txt_columns=[
                "commentaires_complements",
                "intitule_de_l_accompagnement",
                "ressources_documentaires",
                "service_bureau",
                "sous_dir_bureau_",
                "porteur_metier",
            ],
            date_columns=[
                "debut_previsionnel_de_l_accompagnement",
                "fin_previsionnelle_de_l_accompagnement",
                "date_de_cloture_questionnaire",
            ],
            ref_columns=["id_direction"],
        ),
        execution_options=config.execution_options["accompagnement_dsci"],
    )
    effectif_dsci = create_task(
        pipeline=_grist_pipeline(
            "effectif_dsci",
            process.process_effectif_dsci,
            cols_mapping={"bureau": "id_bureau", "pole": "id_pole"},
            cols_to_keep=[
                "id",
                "mail",
                "bureau",
                "pole",
                "nom_complet",
                "agent_present",
                "fonction",
                "absent_depuis",
            ],
            date_columns=["absent_depuis"],
            ref_columns=["id_bureau", "id_pole"],
        ),
        execution_options=config.execution_options["effectif_dsci"],
    )
    accompagnement_dsci_equipe = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_dsci_equipe",
            process.process_accompagnement_dsci_equipe,
            cols_mapping={
                "id": "id_accompagnement",
                "equipe_s_dsci": "id_equipe_s_dsci",
            },
            cols_to_keep=["id", "equipe_s_dsci"],
            ref_columns=["id_accompagnement"],
        ),
        execution_options=config.execution_options["accompagnement_dsci_equipe"],
    )
    accompagnement_dsci_porteur = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_dsci_porteur",
            process.process_accompagnement_dsci_porteur,
            cols_mapping={
                "id": "id_accompagnement",
                "porteur_dsci": "id_porteur_dsci",
            },
            cols_to_keep=["id", "porteur_dsci"],
            ref_columns=["id_accompagnement"],
        ),
        execution_options=config.execution_options["accompagnement_dsci_porteur"],
    )
    accompagnement_dsci_typologie = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_dsci_typologie",
            process.process_accompagnement_dsci_typologie,
            cols_mapping={
                "id": "id_accompagnement",
                "typologie": "id_typologie",
            },
            cols_to_keep=["id", "typologie"],
            ref_columns=["id_accompagnement"],
        ),
        execution_options=config.execution_options["accompagnement_dsci_typologie"],
    )
    # Ordre des tâches
    chain(
        [
            accompagnement_dsci(),
            effectif_dsci(),
            accompagnement_dsci_equipe(),
            accompagnement_dsci_porteur(),
            accompagnement_dsci_typologie(),
        ]
    )


@task_group
def mission_innovation() -> None:
    accompagnement_mi = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_mi",
            process.process_accompagnement_mi,
            cols_mapping={
                "direction": "id_direction",
                "pole": "id_pole",
                "type_d_accompagnement": "id_type_d_accompagnement",
            },
            cols_to_keep=[
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
            ],
            txt_columns=["informations_complementaires"],
            date_columns=["date_de_realisation"],
            ref_columns=["id_direction", "id_pole", "id_type_d_accompagnement"],
        ),
        execution_options=config.execution_options["accompagnement_mi"],
    )
    accompagnement_mi_satisfaction = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_mi_satisfaction",
            process.process_accompagnement_mi_satisfaction,
            cols_mapping={
                "accompagnement": "id_accompagnement",
                "type_d_accompagnement": "id_type_d_accompagnement",
            },
            cols_to_keep=[
                "id",
                "accompagnement",
                "type_d_accompagnement",
                "nombre_de_participants",
                "nombre_de_reponses",
                "taux_de_reponse",
                "note_moyenne_de_satisfaction",
                "unite",
            ],
            num_columns=[
                "nombre_de_participants",
                "nombre_de_reponses",
                "taux_de_reponse",
                "note_moyenne_de_satisfaction",
            ],
            ref_columns=["id_accompagnement", "id_type_d_accompagnement"],
        ),
        execution_options=config.execution_options["accompagnement_mi_satisfaction"],
    )
    animateur_interne = create_task(
        pipeline=_grist_pipeline(
            "animateur_interne",
            process.process_animateur_interne,
            cols_mapping={
                "accompagnement": "id_accompagnement",
                "animateur": "id_animateur",
            },
            ref_columns=["id_accompagnement", "id_animateur"],
        ),
        execution_options=config.execution_options["animateur_interne"],
    )
    animateur_externe = create_task(
        pipeline=_grist_pipeline(
            "animateur_externe",
            process.process_animateur_externe,
            cols_mapping={"accompagnement": "id_accompagnement"},
            ref_columns=["id_accompagnement"],
        ),
        execution_options=config.execution_options["animateur_externe"],
    )
    animateur_fac = create_task(
        pipeline=_grist_pipeline(
            "animateur_fac",
            process.process_animateur_fac,
            cols_mapping={
                "accompagnement": "id_accompagnement",
                "animateur": "id_animateur",
            },
            cols_to_keep=[
                "id",
                "accompagnement",
                "animateur",
            ],
            ref_columns=["id_accompagnement", "id_animateur"],
        ),
        execution_options=config.execution_options["animateur_fac"],
    )
    animateur_fac_certification = create_task(
        pipeline=_grist_pipeline(
            "animateur_fac_certification",
            process.process_animateur_fac_certification,
            cols_mapping={
                "id": "id_animateur_fac",
                "certifications_souhaitees": "id_certifications_souhaitees",
            },
            cols_to_keep=["id", "certifications_souhaitees"],
            ref_columns=["id_animateur_fac"],
        ),
        execution_options=config.execution_options["animateur_fac_certification"],
    )
    animateur_fac_certification_valide = create_task(
        pipeline=_grist_pipeline(
            "animateur_fac_certification_valide",
            process.process_animateur_fac_certification_valide,
            cols_mapping={
                "id": "id_animateur_fac",
                "certifications_validees": "id_certifications_validees",
            },
            cols_to_keep=["id", "certifications_validees"],
            ref_columns=["id_animateur_fac"],
        ),
        execution_options=config.execution_options["animateur_fac_certification_valide"],
    )
    laboratoires_territoriaux = create_task(
        pipeline=_grist_pipeline(
            "laboratoires_territoriaux",
            process.process_laboratoires_territoriaux,
            cols_mapping={"direction": "id_direction", "region": "id_region"},
            ref_columns=["id_direction", "id_region"],
        ),
        execution_options=config.execution_options["laboratoires_territoriaux"],
    )
    pleniere_quest_inscription = create_task(
        pipeline=_grist_pipeline(
            "pleniere_quest_inscription",
            process.process_pleniere_quest_inscription,
            cols_mapping={
                "direction": "id_direction",
                "id_accompagnement": "id_id_accompagnement",
                "pleniere": "id_pleniere",
            },
            ref_columns=["id_direction", "id_id_accompagnement", "id_pleniere"],
        ),
        execution_options=config.execution_options["pleniere_quest_inscription"],
    )
    pleniere_quest_satisfaction = create_task(
        pipeline=_grist_pipeline(
            "pleniere_quest_satisfaction",
            process.process_pleniere_quest_satisfaction,
            txt_columns=[
                "mail",
                "ce_que_j_ai_apprecie",
                "ce_qui_peut_etre_ameliore",
            ],
        ),
        execution_options=config.execution_options["pleniere_quest_satisfaction"],
    )
    passinnov_quest_inscription = create_task(
        pipeline=_grist_pipeline(
            "passinnov_quest_inscription",
            process.process_passinnov_quest_inscription,
            cols_mapping={
                "direction": "id_direction",
                "region": "id_region",
                "passinnov": "id_passinnov",
                "id_accompagnement": "id_id_accompagnement",
            },
            cols_to_keep=["id", "mail", "direction", "region", "role", "passinnov", "id_accompagnement"],
            ref_columns=["id_direction", "id_region", "id_passinnov", "id_id_accompagnement"],
        ),
        execution_options=config.execution_options["passinnov_quest_inscription"],
    )
    passinnov_quest_satisfaction = create_task(
        pipeline=_grist_pipeline(
            "passinnov_quest_satisfaction",
            process.process_passinnov_quest_satisfaction,
            cols_mapping={
                "quest_passinnov": "id_quest_passinnov",
                "id_passinnov": "id_id_passinnov",
            },
            ref_columns=["id_quest_passinnov", "id_id_passinnov"],
        ),
        execution_options=config.execution_options["passinnov_quest_satisfaction"],
    )
    formation_codev_quest_inscription = create_task(
        pipeline=_grist_pipeline(
            "formation_codev_quest_inscription",
            process.process_formation_codev_quest_inscription,
            cols_mapping={
                "direction": "id_direction",
                "id_accompagnement": "id_id_accompagnement",
                "session_formation_codev": "id_session_formation_codev",
            },
            cols_to_keep=[
                "id",
                "mail",
                "direction",
                "formation_codev",
                "experience_codev",
                "details_experience",
                "difficultes",
                "attentes",
                "session_formation_codev",
                "id_accompagnement",
            ],
            txt_columns=[
                "formation_codev",
                "experience_codev",
                "details_experience",
                "difficultes",
                "attentes",
            ],
            ref_columns=[
                "id_direction",
                "id_id_accompagnement",
                "id_session_formation_codev",
            ],
        ),
        execution_options=config.execution_options["formation_codev_quest_inscription"],
    )
    formation_fac_quest_satisfaction = create_task(
        pipeline=_grist_pipeline(
            "formation_fac_quest_satisfaction",
            process.process_formation_fac_quest_satisfaction,
            cols_mapping={
                "quest_formation": "id_quest_formation",
                "promotion": "id_promotion",
                "id_formation": "id_id_formation",
            },
            cols_to_keep=[
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
            ],
            txt_columns=[
                "commentaire_m1",
                "commentaire_m2",
                "commentaire_m3",
                "utilite",
                "besoin",
            ],
            ref_columns=[
                "id_quest_formation",
                "id_promotion",
                "id_id_formation",
            ],
        ),
        execution_options=config.execution_options["formation_fac_quest_satisfaction"],
    )
    formation_fac_envie_suite_quest_satisfaction = create_task(
        pipeline=_grist_pipeline(
            "formation_fac_envie_suite_quest_satisfaction",
            process.process_formation_fac_envie_suite_quest_satisfaction,
            cols_mapping={"id": "id_formation_fac"},
            cols_to_keep=["id", "envies_pour_la_suite"],
            ref_columns=["id_formation_fac"],
        ),
        execution_options=config.execution_options["formation_fac_envie_suite_quest_satisfaction"],
    )
    fac_hors_bercylab_quest_accompagnement = create_task(
        pipeline=_grist_pipeline(
            "fac_hors_bercylab_quest_accompagnement",
            process.process_fac_hors_bercylab_quest_accompagnement,
            cols_mapping={
                "direction": "id_direction",
                "region": "id_region",
                "facilitateur_1": "id_facilitateur_1",
                "facilitateur_2": "id_facilitateur_2",
                "facilitateur_3": "id_facilitateur_3",
                "facilitateurs": "id_facilitateurs",
            },
            cols_to_keep=[
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
            ],
            date_columns=["date_de_realisation"],
            txt_columns=[
                "synthese_de_l_accompagnement",
                "intitule_de_l_accompagnement",
            ],
            ref_columns=[
                "id_direction",
                "id_region",
                "id_facilitateur_1",
                "id_facilitateur_2",
                "id_facilitateur_3",
            ],
        ),
        execution_options=config.execution_options["fac_hors_bercylab_quest_accompagnement"],
    )
    fac_hors_bercylab_quest_type_accompagnement = create_task(
        pipeline=_grist_pipeline(
            "fac_hors_bercylab_quest_type_accompagnement",
            process.process_fac_hors_bercylab_quest_type_accompagnement,
            cols_mapping={"id": "id_formation_fac_hors_bercylab"},
            cols_to_keep=["id", "type_d_accompagnement"],
            ref_columns=["id_formation_fac_hors_bercylab"],
        ),
        execution_options=config.execution_options["fac_hors_bercylab_quest_type_accompagnement"],
    )
    fac_hors_bercylab_quest_accompagnement_participants = create_task(
        pipeline=_grist_pipeline(
            "fac_hors_bercylab_quest_accompagnement_participants",
            process.process_fac_hors_bercylab_quest_accompagnement_participants,
            cols_mapping={"id": "id_formation_fac_hors_bercylab"},
            cols_to_keep=["id", "participants"],
            ref_columns=["id_formation_fac_hors_bercylab"],
        ),
        execution_options=config.execution_options["fac_hors_bercylab_quest_accompagnement_participants"],
    )
    fac_hors_bercylab_quest_accompagnement_facilitateurs = create_task(
        pipeline=_grist_pipeline(
            "fac_hors_bercylab_quest_accompagnement_facilitateurs",
            process.process_fac_hors_bercylab_quest_accompagnement_facilitateurs,
            cols_mapping={
                "id": "id_formation_fac_hors_bercylab",
                "facilitateurs": "id_facilitateurs",
            },
            cols_to_keep=["id", "facilitateurs"],
            ref_columns=["id_formation_fac_hors_bercylab"],
        ),
        execution_options=config.execution_options["fac_hors_bercylab_quest_accompagnement_facilitateurs"],
    )

    # Ordre des tâches
    chain(
        [
            accompagnement_mi(),
            accompagnement_mi_satisfaction(),
            animateur_interne(),
            animateur_externe(),
            animateur_fac(),
            animateur_fac_certification(),
            animateur_fac_certification_valide(),
            laboratoires_territoriaux(),
            pleniere_quest_inscription(),
            pleniere_quest_satisfaction(),
            passinnov_quest_inscription(),
            passinnov_quest_satisfaction(),
            formation_codev_quest_inscription(),
            formation_fac_quest_satisfaction(),
            formation_fac_envie_suite_quest_satisfaction(),
            fac_hors_bercylab_quest_accompagnement(),
            fac_hors_bercylab_quest_type_accompagnement(),
            fac_hors_bercylab_quest_accompagnement_participants(),
            fac_hors_bercylab_quest_accompagnement_facilitateurs(),
        ]
    )


@task_group
def conseil_interne() -> None:
    accompagnement_cci_opportunite = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_cci_opportunite",
            process.process_accompagnement_cci_opportunite,
            cols_mapping={"accompagnement": "id_accompagnement"},
            date_columns=[
                "date_de_reception",
                "date_de_proposition_d_accompagnement",
                "date_prise_de_decision",
            ],
            ref_columns=["id_accompagnement"],
        ),
        execution_options=config.execution_options["accompagnement_cci_opportunite"],
    )
    charge_agent_cci = create_task(
        pipeline=_grist_pipeline(
            "charge_agent_cci",
            process.process_charge_agent_cci,
            cols_mapping={
                "agent_e_": "id_agent_e_",
                "semaine": "id_semaine",
                "missions": "id_missions",
            },
            num_columns=["temps_passe", "taux_de_charge"],
            ref_columns=["id_agent_e_", "id_semaine", "id_missions"],
        ),
        execution_options=config.execution_options["charge_agent_cci"],
    )
    accompagnement_cci_quest_satisfaction = create_task(
        pipeline=_grist_pipeline(
            "accompagnement_cci_quest_satisfaction",
            process.process_accompagnement_cci_quest_satisfaction,
            cols_mapping={
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
            },
            ref_columns=[
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
            ],
        ),
        execution_options=config.execution_options["accompagnement_cci_quest_satisfaction"],
    )

    # Ordre des tâches
    chain(
        [
            accompagnement_cci_opportunite(),
            charge_agent_cci(),
            accompagnement_cci_quest_satisfaction(),
        ]
    )
