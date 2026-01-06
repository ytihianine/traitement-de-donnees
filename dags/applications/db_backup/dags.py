from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain

from utils.config.dag_params import create_dag_params, create_default_args
from infra.mails.default_smtp import create_airflow_callback, MailStatus

from utils.config.types import DagStatus
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.applications.db_backup.tasks import validate_params, dump_databases


LINK_DOC_PIPELINE = "TO COMPLETE"
LINK_DOC_DATA = "Aucune"


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
        nom_projet="Sauvegarde databases",
        dag_status=DagStatus.RUN.value,
        prod_schema="null",
        mail_enable=False,
        mail_to=["yanis.tihianine@finances.gouv.fr"],
        mail_cc=["labo-data@finances.gouv.fr"],
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
    ),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def sauvegarde_database() -> None:
    """Task order"""
    chain(
        validate_params(),
        dump_databases(),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
    )


sauvegarde_database()
