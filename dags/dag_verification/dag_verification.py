from pprint import pprint

from airflow.sdk import dag, get_current_context, task
from airflow.sdk.bases.operator import chain
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
from modules.domain.dataset.model import DatasetContext
from modules.infra.airflow.common_tasks.projet import (
    get_projet_datasets_context,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import (
    create_dag_params,
    create_default_args,
)

nom_projet = "Configuration des projets"


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
    @task
    def print_context(**context) -> None:
        pprint(object=context)
        pprint(object=context["dag"].__dict__)
        pprint(object=context["ti"].__dict__)

    datasets_context = get_projet_datasets_context(nom_projet=nom_projet)

    @task(map_index_template="{{ dataset_name }}")
    def print_dataset_context(
        dataset_context: DatasetContext,
        **context,
    ) -> None:
        context = get_current_context()
        context["dataset_name"] = dataset_context.dataset_name  # type: ignore
        print(f"Dataset: {dataset_context.dataset_name}")
        print(f"Source location: {dataset_context.src_location}")
        print(f"Temporary location: {dataset_context.tmp_location}")
        print(f"Destination location: {dataset_context.dest_location}")

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        datasets_context,
        [
            print_context(),
            check_dag(),
            check_projet(),
            check_dataset(),
            check_pipeline(),
            check_db_interface(),
            check_fs_interface(),
            check_iceberg_catalog(),
            check_grist(),
            check_http_interface(),
            check_smtp(),
            print_dataset_context.expand(dataset_context=datasets_context),
        ],
    )


dag_verification()
