from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_list_selector_info
from enums.dags import DagStatus
from utils.tasks.validation import validate_dag_parameters
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    import_file_to_db,
)
from utils.tasks.s3 import copy_s3_files, del_s3_files

from dags.applications.catalogue.grist.tasks import (
    referentiels_grist,
    source_grist,
)


nom_projet = "Catalogue"


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
def catalogue() -> None:
    """Task order"""
    chain(
        validate_dag_parameters(),
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
            selecteur_info=get_list_selector_info(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
    )


catalogue()
