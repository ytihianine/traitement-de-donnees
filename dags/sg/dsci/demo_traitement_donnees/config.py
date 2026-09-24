from modules.types.projet import SelecteurStorageOptions

storage_options = {
    "accompagnement": SelecteurStorageOptions(tbl_order=5),
    # Référentiels
    "ref_direction": SelecteurStorageOptions(tbl_order=1),
    "ref_intervention": SelecteurStorageOptions(tbl_order=1),
}
