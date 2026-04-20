from typing import Literal

from airflow.sdk import dag, task

from src._types.dags import FeatureFlagsEnable
from src.utils.config.dag_params import create_dag_params, create_default_args
from src._enums.dags import DagStatus

nom_projet = "Liste des packages installés"


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
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=None,
        feature_flags=FeatureFlagsEnable(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
)
def liste_packages() -> None:
    @task.bash
    def bash_task() -> Literal["pip freeze"]:
        return "pip freeze"

    bash_task()


liste_packages()
