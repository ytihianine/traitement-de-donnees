from src._types.projet import SelecteurStorageOptions
from src.enums.database import LoadStrategy
from src.utils.config.vars import DEFAULT_PG_CONFIG_CONN_ID

selecteur_options = {
    "service": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "direction": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "grist_doc": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "projet_contact": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projet_documentation": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projet_s3": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projet_selecteur": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "projets": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_column_mapping": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_database": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_s3": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "selecteur_source": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=False,
        load_strategy=LoadStrategy.APPEND,
    ),
}
