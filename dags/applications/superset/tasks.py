import logging

from airflow.sdk import Variable, task
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.http_client.config import ClientConfig
from modules.infra.http_client.factory import HttpHandlerType, create_http_client
from modules.infra.superset.api import SupersetApiClient


@task
def update_admin_ownership(db_conn_id: str) -> None:
    db_handler = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=db_conn_id),
    )

    # Update table owners
    logging.info(msg="Ajouter tous les admins en tant que propriétaires des tables.")
    query_tbl = """
        INSERT INTO sqlatable_user (user_id, table_id)
        SELECT DISTINCT u.user_id, tbl.id
        FROM tables tbl
        JOIN ab_user_role u ON u.user_id NOT IN
            (SELECT user_id FROM sqlatable_user WHERE table_id = tbl.id)
        WHERE u.role_id = (SELECT id FROM ab_role WHERE name = 'Admin');
    """
    db_handler.execute(query=query_tbl)
    logging.info(msg="Toutes les tables ont été mises à jour.")

    # Update chart owners
    logging.info(msg="Ajouter tous les admins en tant que propriétaires des graphiques.")
    query_tbl = """
        INSERT INTO slice_user (user_id, slice_id)
        SELECT DISTINCT u.user_id, s.id
        FROM slices s
            JOIN ab_user_role u ON u.user_id NOT IN
            (SELECT user_id FROM slice_user WHERE slice_id = s.id)
        WHERE u.role_id = (SELECT id FROM ab_role WHERE name = 'Admin');
    """
    db_handler.execute(query=query_tbl)
    logging.info(msg="Tous les graphiques ont été mis à jour.")

    # Update dashboard owners
    logging.info(msg="Ajouter tous les admins en tant que propriétaires des tableaux de bord.")
    query_tbl = """
        INSERT INTO dashboard_user (user_id, dashboard_id)
        SELECT DISTINCT u.user_id, tdb.id
        FROM dashboards tdb
        JOIN ab_user_role u ON u.user_id NOT IN
            (SELECT user_id FROM dashboard_user WHERE dashboard_id = tdb.id)
        WHERE u.role_id = (SELECT id FROM ab_role WHERE name = 'Admin');
    """
    db_handler.execute(query=query_tbl)
    logging.info(msg="Tous les tableaux de bord ont été mis à jour.")


@task
def sync_dataset_columns() -> None:
    # Create the HTTP client for Superset API
    client_config = ClientConfig()
    http_client = create_http_client(config=client_config, client_type=HttpHandlerType.REQUEST)

    # Initialize the Superset API client
    superset_admin = Variable.get("chartsgouv_admin_username")
    superset_password = Variable.get("chartsgouv_admin_password")
    superset_api_client = SupersetApiClient(
        http_client=http_client,
        username=superset_admin,
        password=superset_password,
    )

    # Get the user token
    token = superset_api_client.get_user_token(
        username=superset_api_client.username,
        password=superset_api_client.password,
    )

    # Sync dataset columns for all datasets
    dataset_ids = superset_api_client.list_datasets(token=token)
    for dataset_id in dataset_ids:
        superset_api_client.sync_dataset_columns(dataset_id=dataset_id, token=token)
