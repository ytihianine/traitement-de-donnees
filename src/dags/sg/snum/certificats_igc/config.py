from src._types.projet import SelecteurStorageOptions

storage_options = {
    "agent": SelecteurStorageOptions(write_to_db=False),
    "certificat": SelecteurStorageOptions(write_to_db=False),
}
