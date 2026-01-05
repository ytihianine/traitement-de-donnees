from airflow.decorators import dag
from airflow.models.baseoperator import chain

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args

# from utils.tasks.s3 import copy_s3_files, del_s3_files

from dags.applications.catalogue.update.tasks import (
    validate_params,
    source_database,
    update_grist_catalogue,
    # update_catalogue,
    # update_dictionnaire,
)
from utils.config.types import DagStatus


nom_projet = "Catalogue - Update"
LINK_DOC_PIPELINE = "Non-défini"  # noqa
LINK_DOC_DATA = "Non-défini"  # noqa


# Définition du DAG
@dag(
    dag_id="catalogue_update",
    schedule_interval="*/15 * * * *",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "CATALOGUE", "DOCUMENTATION"],
    description="""Pour mettre à jour le catalogue Grist.""",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.DEV,
        prod_schema="documentation",
        mail_enable=False,
        mail_to=["yanis.tihianine@finances.gouv.fr"],
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
    ),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
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
        # copy_s3_files(bucket="dsci"),
        # del_s3_files(bucket="dsci"),
    )


catalogue_update()
