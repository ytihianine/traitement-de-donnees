from modules.types.projet import LoadStrategy, SelecteurStorageOptions

storage_options = {
    "agent": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD, read_options={"sep": ";"}),
    "certificat": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "mandataire": SelecteurStorageOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
