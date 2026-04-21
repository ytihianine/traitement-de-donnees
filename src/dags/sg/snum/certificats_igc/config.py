from src._types.projet import SelecteurStorageOptions, LoadStrategy

storage_options = {
    "agent": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "certificat": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "aip": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND),
    "historique_certificat": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND),
    "liste_certificat": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND),
    "mandataire": SelecteurStorageOptions(load_strategy=LoadStrategy.APPEND),
}
