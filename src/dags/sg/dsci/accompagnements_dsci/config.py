from src._types.projet import SelecteurStorageOptions
from src._enums.database import LoadStrategy

storage_options = {
    "accompagnement_cci_opportunite": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "accompagnement_cci_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "accompagnement_dsci": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_dsci_equipe": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "accompagnement_dsci_porteur": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "accompagnement_dsci_typologie": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "accompagnement_mi": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_mi_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "animateur_externe": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "animateur_fac": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "animateur_fac_certification": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "animateur_fac_certification_valide": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "animateur_interne": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "bilaterale": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "bilaterale_remontee": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "charge_agent_cci": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "correspondant": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "correspondant_competence_particuliere": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "correspondant_connaissance_communaute": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "correspondant_profil": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "effectif_dsci": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "fac_hors_bercylab_quest_accompagnement": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
    ),
    "fac_hors_bercylab_quest_accompagnement_facilitateurs": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "fac_hors_bercylab_quest_accompagnement_participants": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "fac_hors_bercylab_quest_type_accompagnement": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "formation_codev_quest_inscription": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "formation_fac_envie_suite_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "formation_fac_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "laboratoires_territoriaux": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "passinnov_quest_inscription": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "passinnov_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "pleniere_quest_inscription": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "pleniere_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
}
