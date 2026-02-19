from pprint import pprint

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import (
    send_mail,
    _callback,
    MailStatus,
    MailMessage,
)
from infra.catalog.iceberg import generate_catalog_properties
from utils.config.dag_params import create_default_args, create_dag_params
from _types.dags import DBParams, FeatureFlags
from enums.dags import DagStatus

from utils.tasks.sql import get_projet_snapshot
from utils.tasks.projet import config_projet_group

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
        db_params=None,
        feature_flags=FeatureFlags(
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
        import pyarrow as pa
        import pandas as pd
        from pyiceberg.catalog import load_catalog

        properties = generate_catalog_properties(
            uri=DEFAULT_POLARIS_HOST,
        )

        catalog = load_catalog(name="data_store", **properties)

        df = pd.DataFrame(data={"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        print("Creating namespace")
        namespace = "testnamespace"
        catalog.create_namespace_if_not_exists(namespace=namespace)
        print("Namespace created")

        print("Creating tbl_name")
        tbl_name = "testnamespace.test_table"
        tbl = catalog.create_table_if_not_exists(
            identifier=tbl_name, schema=pa.Schema.from_pandas(df)
        )
        print("tbl_name created")

        print("Loading data to tbl")
        tbl.append(df=pa.Table.from_pandas(df, preserve_index=False))

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
