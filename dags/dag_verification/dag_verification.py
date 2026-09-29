from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.dag_verification.config import nom_projet, nom_projet_test
from dags.dag_verification.tasks import (
    check_dag,
    check_dataset,
    check_db_interface,
    check_fs_interface,
    check_grist,
    check_http_interface,
    check_iceberg_catalog,
    check_pipeline,
    check_projet,
    check_smtp,
)
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import (
    create_dag_params,
    create_default_args,
)


# Définition du DAG
@dag(
    dag_id="dag_verification",
    schedule=None,
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "Vérification"],
    description="Dag de vérification.",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="iceberg"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=True,
            s3=True,
            convert_files=True,
            download_grist_doc=True,
        ),
    ),
)
def dag_verification() -> None:

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        [
            check_dag(),
            check_projet(),
            check_dataset(nom_projet=nom_projet_test),
            check_pipeline(),
            check_db_interface(),
            check_fs_interface(),
            check_iceberg_catalog(),
            check_grist(),
            check_http_interface(),
            check_smtp(),
        ],
    )


dag_verification()
