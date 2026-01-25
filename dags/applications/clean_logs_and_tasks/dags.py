from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args

from dags.applications.clean_logs_and_tasks.task import (
    clean_s3,
    clean_old_logs,
    clean_skipped_logs,
)
from enums.dags import DagStatus

nom_projet = "Clean tasks and logs"


# Définition du DAG
@dag(
    dag_id="clean_logs_tasks",
    schedule="@weekly",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "LOGS"],
    description="Pipeline qui nettoie la base de données et S3",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=None,
        feature_flags=FeatureFlags(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def clean_logs_tasks() -> None:
    # nom_projet = "Clean tasks and logs"

    """Task definitions"""
    chain(clean_s3(), clean_old_logs(), clean_skipped_logs())


clean_logs_tasks()
