from _types.projet import SelecteurStorageOptions

selecteur_options = {
    "get_agent_db": SelecteurStorageOptions(
        write_to_s3=True,
        write_to_s3_with_iceberg=False,
    ),
    "grist_doc": SelecteurStorageOptions(
        write_to_s3=True,
        write_to_s3_with_iceberg=False,
    ),
    "load_agent": SelecteurStorageOptions(
        write_to_s3=False,
        write_to_s3_with_iceberg=False,
    ),
}
