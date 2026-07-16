from src._enums.database import LoadStrategy
from src._types.projet import SelecteurStorageOptions

storage_options = {
    "service": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "direction": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "grist_doc": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "projet_contact": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projet_documentation": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projet_s3": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projet_selecteur": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projets": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_column_mapping": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_database": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_s3": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_source": SelecteurStorageOptions(
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
}
