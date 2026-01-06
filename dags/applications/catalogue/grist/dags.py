from airflow.decorators import dag
from airflow.models.baseoperator import chain

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_projet_config
from utils.config.types import DagStatus
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    import_file_to_db,
)
from utils.tasks.s3 import copy_s3_files, del_s3_files

from dags.applications.catalogue.grist.tasks import (
    validate_params,
    referentiels_grist,
    source_grist,
)


nom_projet = "Catalogue"
LINK_DOC_PIPELINE = "Non-défini"  # noqa
LINK_DOC_DATA = "Non-défini"  # noqa


# Définition du DAG
@dag(
    dag_id="catalogue",
    schedule="*/15 * * * *",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "CATALOGUE", "DOCUMENTATION"],
    description="""Récupère les données du catalogue Grist.""",
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
def catalogue() -> None:
    """Task order"""
    chain(
        validate_params(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="catalogue",
            doc_id_key="grist_doc_id_catalogue",
        ),
        [
            referentiels_grist(),
            source_grist(),
        ],
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
        delete_tmp_tables(),
    )


catalogue()
