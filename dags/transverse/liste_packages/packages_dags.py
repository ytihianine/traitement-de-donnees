from typing import Literal

from airflow.sdk import dag, task

from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.types import DagStatus


# Définition du DAG
@dag(
    dag_id="liste-des-packages",
    schedule="@once",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "INFO"],
    description="Liste des packages installés dans l'instance",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet="liste-des-packages",
        dag_status=DagStatus.RUN,
        prod_schema="Aucun",
        mail_enable=False,
    ),
)
def liste_packages() -> None:
    @task.bash
    def bash_task() -> Literal["pip freeze"]:
        return "pip freeze"

    bash_task()


liste_packages()
