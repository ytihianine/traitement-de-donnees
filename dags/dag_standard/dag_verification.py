from pprint import pprint

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import (
    send_mail,
    _callback,
    MailStatus,
    MailMessage,
)
from infra.catalog.iceberg import generate_catalog_properties, IcebergCatalog
from utils.config.dag_params import create_default_args, create_dag_params, get_db_info
from _types.dags import DBParams, FeatureFlags
from enums.dags import DagStatus
from enums.filesystem import IcebergTableStatus

from utils.tasks.sql import get_projet_snapshot
from utils.tasks.projet import config_projet_group
from utils.tasks.s3 import write_to_s3

from utils.config.vars import DEFAULT_POLARIS_HOST

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
        feature_flags=FeatureFlags(
            db=True,
            mail=False,
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
        pprint(
            object=context["ti"].xcom_pull(
                key="snapshot_id", task_ids="get_projet_snapshot"
            )
        )

    @task
    def send_simple_mail(**context) -> None:
        mail_message = MailMessage(
            to=["yanis.tihianine@finances.gouv.fr"],
            cc=["yanis.tihianine@finances.gouv.fr"],
            subject="Simple test depuis Airflow",
            html_content="Test réussi !",
        )
        send_mail(mail_message=mail_message)

    @task
    def send_error_mail(**context) -> None:
        _callback(context=context, mail_status=MailStatus.ERROR)

    @task
    def send_success_mail(**context) -> None:
        _callback(context=context, mail_status=MailStatus.SUCCESS)

    @task
    def iceberg_task(**context) -> None:
        import pandas as pd

        namespace = get_db_info(context=context).prod_schema
        properties = generate_catalog_properties(
            uri=DEFAULT_POLARIS_HOST,
        )
        catalog = IcebergCatalog(name="data_store", properties=properties)

        df = pd.DataFrame(data={"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        write_to_s3(
            catalog=catalog,
            df=df,
            table_status=IcebergTableStatus.PROD,
            namespace=namespace,
            key="my_table_test",
        )

    # Ordre des tâches
    chain(
        [
            get_projet_snapshot(),
            print_context(),
            send_simple_mail(),
            send_error_mail(),
            send_success_mail(),
            config_projet_group(nom_projet=nom_projet),
            iceberg_task(),
        ],
    )


dag_verification()
