from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from datetime import timedelta

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from src._enums.dags import DagStatus
from src._types.dags import DBParams, FeatureFlagsEnable
from src.common_tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
)
from src.common_tasks.validation import validate_dag_parameters
from src.common_tasks.projet import get_selecteur_config
from src.utils.config.dag_params import create_dag_params, create_default_args
from src.common_tasks.grist import download_grist_doc_to_s3
from src.common_tasks.s3 import del_s3_files

from dags.cgefi.suivi_activite.tasks import (
    referentiels,
    informations_generales,
    processus_4,
    processus_6,
    processus_atpro,
)

nom_projet = "Emploi et formation"


# Définition du DAG
@dag(
    dag_id="cgefi_suivi_activite",
    schedule="*/5 * * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["CGEFI", "ACTIVITE", "POC", "TABLEAU DE BORD"],
    description="""Pipeline qui récupère les nouvelles données dans Grist
        pour actualiser le tableau de bord de suivi d'activité du CGEFI""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=1, retry_delay=timedelta(seconds=30)),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="cgefi_poc"),
        feature_flags=FeatureFlagsEnable(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def suivi_activite() -> None:
    """Task order"""
    selecteur_configs = get_selecteur_config(selecteur_mapping={})
    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci-cgefi",
            doc_id_key="grist_doc_id_cgefi_suivi_activite",
        ),
        [
            referentiels(),
            informations_generales(),
            processus_4(),
            processus_6(),
            processus_atpro(),
        ],
        create_tmp_tables(),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(),
        del_s3_files(),
        delete_tmp_tables(),
    )


suivi_activite()
