from src._types.projet import SelecteurStorageOptions, LoadStrategy

storage_options = {
    "agent": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "certificat": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "mandataire": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
