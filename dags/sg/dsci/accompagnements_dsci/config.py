from _types.projet import SelecteurStorageOptions
from enums.database import LoadStrategy

selecteur_options = {
    "accompagnement_cci_opportunite": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_cci_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_dsci": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_dsci_equipe": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_dsci_porteur": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_dsci_typologie": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_mi": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "accompagnement_mi_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "animateur_externe": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "animateur_fac": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "animateur_fac_certification": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "animateur_fac_certification_valide": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "animateur_interne": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "bilaterale": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "bilaterale_remontee": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "charge_agent_cci": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "correspondant": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "correspondant_competence_particuliere": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "correspondant_connaissance_communaute": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "correspondant_profil": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "effectif_dsci": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "fac_hors_bercylab_quest_accompagnement": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "fac_hors_bercylab_quest_accompagnement_facilitateurs": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "fac_hors_bercylab_quest_accompagnement_participants": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "fac_hors_bercylab_quest_type_accompagnement": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "formation_codev_quest_inscription": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "formation_fac_envie_suite_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "formation_fac_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "laboratoires_territoriaux": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "passinnov_quest_inscription": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "passinnov_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "pleniere_quest_inscription": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "pleniere_quest_satisfaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
}
