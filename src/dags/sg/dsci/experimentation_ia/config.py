from src._types.projet import SelecteurStorageOptions
from src._enums.database import LoadStrategy

storage_options = {
    "quota_par_entite": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "experimentateurs": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    # Questionnaire 1
    "questionnaire_1": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "questionnaire_1_cas_usage": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire_1_besoins_accompagnement": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    # Questionnaire 2
    "questionnaire_2": SelecteurStorageOptions(load_strategy=LoadStrategy.INCREMENTAL),
    "questionnaire2_typologie_interaction": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_facteurs_progression": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_formation_suivie": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_freins": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_impact_identifie": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_impact_observe": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_participation": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_taches": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    "questionnaire2_typologie_erreurs": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
    # Questionnaire_2_bis
    "questionnaire_2_bis": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL
    ),
    "questionnaire_2_bis_raisons_non_utilisation": SelecteurStorageOptions(
        load_strategy=LoadStrategy.INCREMENTAL,
        tbl_order=5,
    ),
}
