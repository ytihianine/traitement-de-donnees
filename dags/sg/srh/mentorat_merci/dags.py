from datetime import timedelta

from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.default_smtp import create_send_mail_callback, MailStatus

from enums.dags import DagStatus

from _types.dags import DBParams, FeatureFlags
from utils.config.tasks import get_s3_keys_source
from utils.config.dag_params import create_default_args, create_dag_params

from utils.tasks.validation import validate_dag_parameters
from utils.tasks.s3 import copy_s3_files, del_s3_files

from dags.sg.srh.mentorat_merci.tasks import (
    agent_inscrit,
    generer_binomes,
)


# Mails
nom_projet = "Mentorat MERCI"
LINK_DOC_PIPELINE = "Non-défini"  # noqa
LINK_DOC_DATA = "Non-défini"  # noqa


# Définition du DAG
@dag(
    dag_id="mentorat_merci",
    schedule="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SRH", "Mentorat MERCI"],
    description="Génération de binômes pour le session Mentorat MERCI",  # noqa
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="srh"),
        feature_flags=FeatureFlags(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def mentorat_merci() -> None:
    """Task definition"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.START),
    )

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        looking_for_files,
        agent_inscrit(),
        generer_binomes(),
        copy_s3_files(),
        del_s3_files(),
    )


mentorat_merci()
