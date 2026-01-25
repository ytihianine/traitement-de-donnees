from datetime import timedelta
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from enums.dags import DagStatus
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    delete_tmp_tables,
    refresh_views,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import get_s3_keys_source, get_projet_config

from dags.dge.carto_rem.fichiers.tasks import (
    validate_params,
    source_files,
    output_files,
)


# Mails
nom_projet = "Cartographie rémunération"


# Définition du DAG
@dag(
    dag_id="cartographie_remuneration",
    schedule="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DGE", "RH"],
    description="""DGE - Cartographie rémunération""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.DEV,
        db_params=DBParams(prod_schema="cartographie_remuneration"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration() -> None:
    """Task definitions"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.START),
    )

    """ Task order """
    chain(
        validate_params(),
        looking_for_files,
        source_files(),
        output_files(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        refresh_views(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
    )


cartographie_remuneration()
