from pprint import pprint

from airflow.sdk import dag, task
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import (
    send_mail,
    _callback,
    MailStatus,
    MailMessage,
)
from utils.config.dag_params import create_default_args, create_dag_params
from _types.dags import DBParams, FeatureFlags
from enums.dags import DagStatus

from utils.tasks.sql import get_projet_snapshot
from utils.tasks.projet import config_projet_group

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
        db_params=DBParams(prod_schema="donnee_comptable"),
        feature_flags=FeatureFlags(
            db=True,
            mail=False,
            s3=False,
            convert_files=False,
            download_grist_doc=False,
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

    # Ordre des tâches
    chain(
        [
            get_projet_snapshot(),
            print_context(),
            send_simple_mail(),
            send_error_mail(),
            send_success_mail(),
            config_projet_group(nom_projet=nom_projet),
        ],
    )


dag_verification()
