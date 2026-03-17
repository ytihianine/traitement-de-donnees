from _types.projet import SelecteurStorageOptions
from utils.config.vars import DEFAULT_PG_CONFIG_CONN_ID

selecteur_options = {
    "service": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "direction": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "grist_doc": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "projet_contact": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "projet_documentation": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "projet_s3": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "projet_selecteur": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "projets": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "selecteur_column_mapping": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "selecteur_database": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "selecteur_s3": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
    "selecteur_source": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
        keep_file_id_col=True,
    ),
}
