from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from src.utils.process.structures import normalize_grist_dataframe
from src.common_tasks.etl import create_grist_etl_task

from dags.sg.dsci.accompagnements_dsci import process


# Création des tâches
@task_group
def referentiels() -> None:
    ref_bureau = create_grist_etl_task(
        selecteur="ref_bureau",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_bureau,
    )
    ref_certification = create_grist_etl_task(
        selecteur="ref_certification",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_certification,
    )
    ref_competence_particuliere = create_grist_etl_task(
        selecteur="ref_competence_particuliere",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_competence_particuliere,
    )
    ref_direction = create_grist_etl_task(
        selecteur="ref_direction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_direction,
    )
    ref_profil_correspondant = create_grist_etl_task(
        selecteur="ref_profil_correspondant",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_profil_correspondant,
    )
    ref_qualite_service = create_grist_etl_task(
        selecteur="ref_qualite_service",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_qualite_service,
    )
    ref_region = create_grist_etl_task(
        selecteur="ref_region",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_region,
    )
    ref_semainier = create_grist_etl_task(
        selecteur="ref_semainier",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_semainier,
    )
    ref_typologie_accompagnement = create_grist_etl_task(
        selecteur="ref_typologie_accompagnement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_typologie_accompagnement,
    )
    ref_pole = create_grist_etl_task(
        selecteur="ref_pole",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_pole,
    )
    ref_type_accompagnement = create_grist_etl_task(
        selecteur="ref_type_accompagnement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_ref_type_accompagnement,
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
    bilaterale = create_grist_etl_task(
        selecteur="bilaterale",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_bilaterale,
    )
    bilaterale_remontee = create_grist_etl_task(
        selecteur="bilaterale_remontee",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_bilaterale_remontee,
    )
    # Ordre des tâches
    chain([bilaterale(), bilaterale_remontee()])


@task_group
def correspondant() -> None:
    correspondant = create_grist_etl_task(
        selecteur="correspondant",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_correspondant,
    )
    correspondant_profil = create_grist_etl_task(
        selecteur="correspondant_profil",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_correspondant_profil,
    )
    correspondant_competence_particuliere = create_grist_etl_task(
        selecteur="correspondant_competence_particuliere",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_correspondant_competence_particuliere,
    )
    correspondant_connaissance_communaute = create_grist_etl_task(
        selecteur="correspondant_connaissance_communaute",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_correspondant_connaissance_communaute,
    )
    # correspondant_certification = create_grist_etl_task(
    #     selecteur="correspondant_certification",
    #     process_func=process.process_correspondant_certification,
    # )

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
    accompagnement_dsci = create_grist_etl_task(
        selecteur="accompagnement_dsci",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_accompagnement_dsci,
    )
    effectif_dsci = create_grist_etl_task(
        selecteur="effectif_dsci",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_effectif_dsci,
    )
    accompagnement_dsci_equipe = create_grist_etl_task(
        selecteur="accompagnement_dsci_equipe",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_accompagnement_dsci_equipe,
    )
    accompagnement_dsci_porteur = create_grist_etl_task(
        selecteur="accompagnement_dsci_porteur",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_accompagnement_dsci_porteur,
    )
    accompagnement_dsci_typologie = create_grist_etl_task(
        selecteur="accompagnement_dsci_typologie",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_accompagnement_dsci_typologie,
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
    accompagnement_mi = create_grist_etl_task(
        selecteur="accompagnement_mi",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_accompagnement_mi,
    )
    accompagnement_mi_satisfaction = create_grist_etl_task(
        selecteur="accompagnement_mi_satisfaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_accompagnement_mi_satisfaction,
    )
    animateur_interne = create_grist_etl_task(
        selecteur="animateur_interne",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_animateur_interne,
    )
    animateur_externe = create_grist_etl_task(
        selecteur="animateur_externe",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_animateur_externe,
    )
    animateur_fac = create_grist_etl_task(
        selecteur="animateur_fac",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_animateur_fac,
    )
    animateur_fac_certification = create_grist_etl_task(
        selecteur="animateur_fac_certification",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_animateur_fac_certification,
    )
    animateur_fac_certification_valide = create_grist_etl_task(
        selecteur="animateur_fac_certification_valide",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_animateur_fac_certification_valide,
    )
    laboratoires_territoriaux = create_grist_etl_task(
        selecteur="laboratoires_territoriaux",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_laboratoires_territoriaux,
    )
    pleniere_quest_inscription = create_grist_etl_task(
        selecteur="pleniere_quest_inscription",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_inscription_pleniere,
    )
    pleniere_quest_satisfaction = create_grist_etl_task(
        selecteur="pleniere_quest_satisfaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_satisfaction_pleniere,
    )
    passinnov_quest_inscription = create_grist_etl_task(
        selecteur="passinnov_quest_inscription",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_inscription_passinnov,
    )
    passinnov_quest_satisfaction = create_grist_etl_task(
        selecteur="passinnov_quest_satisfaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_satisfaction_passinnov,
    )
    formation_codev_quest_inscription = create_grist_etl_task(
        selecteur="formation_codev_quest_inscription",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_inscription_formation_codev,
    )
    formation_fac_quest_satisfaction = create_grist_etl_task(
        selecteur="formation_fac_quest_satisfaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_satisfaction_formation_fac,
    )
    formation_fac_envie_suite_quest_satisfaction = create_grist_etl_task(
        selecteur="formation_fac_envie_suite_quest_satisfaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_satisfaction_formation_fac_envies,
    )
    fac_hors_bercylab_quest_accompagnement = create_grist_etl_task(
        selecteur="fac_hors_bercylab_quest_accompagnement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_accompagnement_fac_hors_bercylab,
    )
    fac_hors_bercylab_quest_type_accompagnement = create_grist_etl_task(
        selecteur="fac_hors_bercylab_quest_type_accompagnement",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_accompagnement_fac_hors_bercylab_type,
    )
    fac_hors_bercylab_quest_accompagnement_partiicipants = create_grist_etl_task(
        selecteur="fac_hors_bercylab_quest_accompagnement_participants",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_accompagnement_fac_hors_bercylab_participants,
    )
    fac_hors_bercylab_quest_accompagnement_facilitateurs = create_grist_etl_task(
        selecteur="fac_hors_bercylab_quest_accompagnement_facilitateurs",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_accompagnement_fac_hors_bercylab_facilitateurs,
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
            fac_hors_bercylab_quest_accompagnement_partiicipants(),
            fac_hors_bercylab_quest_accompagnement_facilitateurs(),
        ]
    )


@task_group
def conseil_interne() -> None:
    accompagnement_cci_opportunite = create_grist_etl_task(
        selecteur="accompagnement_cci_opportunite",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_accompagnement_cci_opportunite,
    )
    charge_agent_cci = create_grist_etl_task(
        selecteur="charge_agent_cci",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_charge_agent_cci,
    )
    accompagnement_cci_quest_satisfaction = create_grist_etl_task(
        selecteur="accompagnement_cci_quest_satisfaction",
        normalisation_process_func=normalize_grist_dataframe,
        process_func=process.process_quest_satisfaction_accompagnement_cci,
    )

    # Ordre des tâches
    chain(
        [
            accompagnement_cci_opportunite(),
            charge_agent_cci(),
            accompagnement_cci_quest_satisfaction(),
        ]
    )
