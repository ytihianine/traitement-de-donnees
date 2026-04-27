from src._types.projet import SelecteurStorageOptions

storage_options = {
    "conso_avant_2019": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "conso_statut_fluide_global": SelecteurStorageOptions(
        write_to_db=False,
    ),
}
