from src._types.projet import SelecteurStorageOptions, LoadStrategy

storage_options = {
    "agent": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "certificat": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "mandataire": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
