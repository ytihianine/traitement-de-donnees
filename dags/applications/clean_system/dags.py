from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.applications.clean_system.task import (
    delete_airflow_keys,
    delete_keys_with_date,
    delete_tmp_keys,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args

nom_projet = "Nettoyer les systèmes"


# Définition du DAG
@dag(
    dag_id="nettoyer_les_systemes",
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "OLD", "S3"],
    description="Pipeline qui nettoie les anciens éléments des systèmes externes.",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=None,
        feature_flags=FeatureFlagsEnable(db=True, mail=True, s3=True, convert_files=False, download_grist_doc=True),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def nettoyer_les_systemes() -> None:
    # nom_projet = "Clean tasks and logs"

    """Task definitions"""
    chain(
        validate_dag_parameters(),
        [
            delete_tmp_keys(),
            delete_keys_with_date(),
            delete_airflow_keys(),
        ],
    )


nettoyer_les_systemes()
