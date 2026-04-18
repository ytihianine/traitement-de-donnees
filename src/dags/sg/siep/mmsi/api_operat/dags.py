from airflow.sdk import dag
from datetime import timedelta
from airflow.sdk.bases.operator import chain

from src.infra.mails.default_smtp import create_send_mail_callback, MailStatus
from src._types.dags import DBParams, FeatureFlagsEnable
from src.utils.config.dag_params import create_dag_params, create_default_args
from src.enums.dags import DagStatus

from src.common_tasks.validation import validate_dag_parameters
from src.common_tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
)
from src.common_tasks.projet import get_selecteur_config

from src.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from src.dags.sg.siep.mmsi.api_operat.task import source, output

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
        feature_flags=FeatureFlagsEnable(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def api_operat_ademe() -> None:
    selecteur_configs = get_selecteur_config(selecteur_mapping={})

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        source(),
        output(),
        create_tmp_tables(),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(),
        copy_s3_files(),
        del_s3_files(),
    )


api_operat_ademe()
