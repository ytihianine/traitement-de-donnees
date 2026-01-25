from datetime import timedelta
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from _types.dags import FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from infra.mails.default_smtp import create_send_mail_callback, MailStatus

from enums.dags import DagStatus
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.applications.db_backup.tasks import validate_params, dump_databases


nom_projet = "Sauvegarde databases"


# Définition du DAG
@dag(
    dag_id="sauvegarde_database",
    schedule=timedelta(hours=12),
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "RECETTE", "SAUVEGARDE", "DATABASE"],
    description="""Pipeline qui réalise des sauvegarde de la base de données""",
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
def sauvegarde_database() -> None:
    """Task order"""
    chain(
        validate_params(),
        dump_databases(),
        copy_s3_files(),
        del_s3_files(),
    )


sauvegarde_database()
