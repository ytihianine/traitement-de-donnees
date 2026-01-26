from airflow.sdk import dag
from datetime import timedelta
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_list_selector_info
from enums.dags import DagStatus

from utils.tasks.validation import validate_dag_parameters
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.sg.siep.mmsi.api_operat.task import taches


nom_projet = "API Opera"


# Définition du DAG
@dag(
    dag_id="api_operat_ademe",
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "ADEME"],
    description="Pipeline qui réalise des appels sur l'API Operat (ADEME)",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=1, retry_delay=timedelta(minutes=1)),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.DEV,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def api_operat_ademe() -> None:

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        taches(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_info=get_list_selector_info(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        copy_s3_files(),
        del_s3_files(),
    )


api_operat_ademe()
