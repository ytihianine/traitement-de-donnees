from airflow.sdk import chain, task_group
from dags.applications.db_backup.actions import export_database


@task_group()
def export_databases():
    airflow_config = export_database(db_name="airflow_config", conn_id="airflow_config_conn")
    superset_config = export_database(db_name="superset_config", conn_id="superset_config_conn")
    data_store = export_database(db_name="data_store", conn_id="data_store_conn")

    chain(
        airflow_config,
        superset_config,
        data_store,
    )
