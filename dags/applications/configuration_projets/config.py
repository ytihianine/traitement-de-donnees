from _types.projet import SelecteurStorageOptions

selecteur_options = {
    "service": SelecteurStorageOptions(),
    "direction": SelecteurStorageOptions(),
    "grist_doc": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "projet_contact": SelecteurStorageOptions(),
    "projet_documentation": SelecteurStorageOptions(),
    "projet_s3": SelecteurStorageOptions(),
    "projet_selecteur": SelecteurStorageOptions(),
    "projets": SelecteurStorageOptions(),
    "selecteur_column_mapping": SelecteurStorageOptions(),
    "selecteur_database": SelecteurStorageOptions(),
    "selecteur_s3": SelecteurStorageOptions(),
    "selecteur_source": SelecteurStorageOptions(),
}
