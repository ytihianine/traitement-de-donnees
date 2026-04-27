from src._types.projet import SelecteurStorageOptions

storage_options = {
    "accompagnement_cci_opportunite": SelecteurStorageOptions(tbl_order=6),
    "accompagnement_cci_quest_satisfaction": SelecteurStorageOptions(tbl_order=7),
    "accompagnement_dsci": SelecteurStorageOptions(tbl_order=5),
    "accompagnement_dsci_equipe": SelecteurStorageOptions(tbl_order=6),
    "accompagnement_dsci_porteur": SelecteurStorageOptions(tbl_order=6),
    "accompagnement_dsci_typologie": SelecteurStorageOptions(tbl_order=6),
    "accompagnement_mi": SelecteurStorageOptions(tbl_order=5),
    "accompagnement_mi_satisfaction": SelecteurStorageOptions(tbl_order=6),
    "animateur_externe": SelecteurStorageOptions(tbl_order=6),
    "animateur_fac": SelecteurStorageOptions(tbl_order=6),
    "animateur_fac_certification": SelecteurStorageOptions(tbl_order=6),
    "animateur_fac_certification_valide": SelecteurStorageOptions(tbl_order=6),
    "animateur_interne": SelecteurStorageOptions(tbl_order=6),
    "bilaterale": SelecteurStorageOptions(tbl_order=5),
    "bilaterale_remontee": SelecteurStorageOptions(tbl_order=6),
    "charge_agent_cci": SelecteurStorageOptions(),
    "correspondant": SelecteurStorageOptions(tbl_order=6),
    "correspondant_competence_particuliere": SelecteurStorageOptions(tbl_order=7),
    "correspondant_connaissance_communaute": SelecteurStorageOptions(tbl_order=7),
    "correspondant_profil": SelecteurStorageOptions(tbl_order=7),
    "effectif_dsci": SelecteurStorageOptions(tbl_order=3),
    "fac_hors_bercylab_quest_accompagnement": SelecteurStorageOptions(tbl_order=6),
    "fac_hors_bercylab_quest_accompagnement_facilitateurs": SelecteurStorageOptions(
        tbl_order=6
    ),
    "fac_hors_bercylab_quest_accompagnement_participants": SelecteurStorageOptions(
        tbl_order=6
    ),
    "fac_hors_bercylab_quest_type_accompagnement": SelecteurStorageOptions(tbl_order=6),
    "formation_codev_quest_inscription": SelecteurStorageOptions(tbl_order=6),
    "formation_fac_envie_suite_quest_satisfaction": SelecteurStorageOptions(
        tbl_order=6
    ),
    "formation_fac_quest_satisfaction": SelecteurStorageOptions(tbl_order=6),
    "grist_doc": SelecteurStorageOptions(write_to_db=False),
    "laboratoires_territoriaux": SelecteurStorageOptions(tbl_order=7),
    "passinnov_quest_inscription": SelecteurStorageOptions(tbl_order=6),
    "passinnov_quest_satisfaction": SelecteurStorageOptions(tbl_order=7),
    "pleniere_quest_inscription": SelecteurStorageOptions(tbl_order=6),
    "pleniere_quest_satisfaction": SelecteurStorageOptions(tbl_order=7),
    # Référentiels
    "ref_bureau": SelecteurStorageOptions(tbl_order=1),
    "ref_certification": SelecteurStorageOptions(tbl_order=1),
    "ref_competence_particuliere": SelecteurStorageOptions(tbl_order=1),
    "ref_direction": SelecteurStorageOptions(tbl_order=1),
    "ref_pole": SelecteurStorageOptions(tbl_order=2),
    "ref_profil_correspondant": SelecteurStorageOptions(tbl_order=1),
    "ref_qualite_service": SelecteurStorageOptions(tbl_order=1),
    "ref_region": SelecteurStorageOptions(tbl_order=1),
    "ref_semainier": SelecteurStorageOptions(tbl_order=1),
    "ref_type_accompagnement": SelecteurStorageOptions(tbl_order=3),
    "ref_typologie_accompagnement": SelecteurStorageOptions(tbl_order=1),
}
