from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args

from dags.applications.clean_logs_and_tasks.task import (
    clean_s3,
    clean_old_logs,
    clean_skipped_logs,
)
from utils.config.types import DagStatus

# Liens
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/catalogue?ref_type=heads"  # noqa
LINK_DOC_DATA = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"  # noqa
)


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
        nom_projet="Clean tasks, logs and S3",
        dag_status=DagStatus.DEV,
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
def clean_logs_tasks() -> None:
    # nom_projet = "Clean tasks and logs"

    """Task definitions"""
    chain(clean_s3(), clean_old_logs(), clean_skipped_logs())


clean_logs_tasks()
