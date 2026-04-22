from src._types.projet import SelecteurStorageOptions, LoadStrategy

storage_options = {
    "agent": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "certificat": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "aip": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "historique_certificat": SelecteurStorageOptions(
        load_strategy=LoadStrategy.FULL_LOAD
    ),
    "liste_certificat": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "mandataire": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
