from airflow import DAG
from airflow.sdk import dag, task
from datetime import datetime, timedelta

import pendulum

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.today(tz="UTC").add(days=-1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# Définition du DAG
@dag(
    "liste-des-packages",
    schedule="@once",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "INFO"],
    description="Liste des packages installés dans l'instance",
    default_args=default_args,
)
def liste_packages():
    @task.bash
    def bash_task():
        return "pip freeze"

    bash_task()


liste_packages()
