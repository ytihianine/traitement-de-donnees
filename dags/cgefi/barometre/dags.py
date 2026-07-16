from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from dags.cgefi.barometre.tasks import (
    source_files,
)
from project.common_tasks.projet import get_list_source_fichier, get_selecteur_config
from project.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from project.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_tmp_tables,
    import_file_to_db,
    # set_dataset_last_update_date,
)
from project.common_tasks.validation import validate_dag_parameters
from project.enums.dags import DagStatus
from project.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from project.types.dags import DBParams, FeatureFlagsEnable
from project.utils.config.dag_params import create_dag_params, create_default_args

nom_projet = "Baromètre"


# Définition du DAG
@dag(
    dag_id="Barometre",
    schedule="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["CGEFI", "BAROMETRE"],
    description="""Pipeline de traitement des données pour le Baromètre""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="cgefi"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def barometre() -> None:
    """Tasks definition"""

    selecteur_configs = get_selecteur_config(storage_options={})

    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_list_source_fichier(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.START),
    )

    end_task = EmptyOperator(
        task_id="end_task",
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
    )

    """ Task order """
    chain(
        validate_dag_parameters(),
        selecteur_configs,
        looking_for_files,
        source_files(),
        create_tmp_tables(),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(),
        copy_s3_files(),
        del_s3_files(),
        end_task,
    )


barometre()
