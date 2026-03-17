from _types.projet import SelecteurStorageOptions
from utils.config.vars import DEFAULT_PG_CONFIG_CONN_ID

selecteur_options = {
    "service": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "direction": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "grist_doc": SelecteurStorageOptions(
        write_to_db=False,
    ),
    "projet_contact": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "projet_documentation": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "projet_s3": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "projet_selecteur": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "projets": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "selecteur_column_mapping": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "selecteur_database": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "selecteur_s3": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
    "selecteur_source": SelecteurStorageOptions(
        db_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
    ),
}
