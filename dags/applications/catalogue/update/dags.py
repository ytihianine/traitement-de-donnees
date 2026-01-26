from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args

# from utils.tasks.s3 import copy_s3_files, del_s3_files

from dags.applications.catalogue.update.tasks import (
    validate_params,
    source_database,
    update_grist_catalogue,
    # update_catalogue,
    # update_dictionnaire,
)
from enums.dags import DagStatus


nom_projet = "Catalogue - Update"


# Définition du DAG
@dag(
    dag_id="catalogue_update",
    schedule="*/15 * * * *",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "CATALOGUE", "DOCUMENTATION"],
    description="""Pour mettre à jour le catalogue Grist.""",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="documentation"),
        feature_flags=FeatureFlags(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def catalogue_update() -> None:
    """Task order"""
    chain(
        validate_params(),
        source_database(),
        update_grist_catalogue(),
        # [
        #     update_catalogue(),
        #     update_dictionnaire(),
        # ],
        # copy_s3_files(),
        # del_s3_files(),
    )


catalogue_update()
