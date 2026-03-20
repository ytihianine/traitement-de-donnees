from _types.projet import SelecteurStorageOptions


selecteur_options = {
    "agent_carriere": SelecteurStorageOptions(
        write_to_s3=False,
        write_to_s3_with_iceberg=False,
    ),
    "agent": SelecteurStorageOptions(
        write_to_s3=False,
        write_to_s3_with_iceberg=False,
    ),
    "agent_elem_rem": SelecteurStorageOptions(
        write_to_s3=False,
        write_to_s3_with_iceberg=False,
    ),
}
