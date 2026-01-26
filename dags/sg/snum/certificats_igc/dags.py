from datetime import timedelta
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from enums.dags import DagStatus
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    create_projet_snapshot,
    get_projet_snapshot,
    import_file_to_db,
    ensure_partition,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import get_list_source_fichier_key, get_list_selector_info

from utils.tasks.validation import validate_dag_parameters
from dags.sg.snum.certificats_igc.tasks import source_files, output_files


nom_projet = "Certificat IGC"


# Définition du DAG
@dag(
    dag_id="certificats_igc",
    schedule="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "SNUM"],
    description="""SG - Certificat IGC""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.DEV,
        db_params=DBParams(prod_schema="certificat_igc"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def certificats_igc() -> None:

    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_list_source_fichier_key(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.START),
    )

    """ Task order """
    chain(
        validate_dag_parameters(),
        looking_for_files,
        create_projet_snapshot(),
        get_projet_snapshot(),
        source_files(),
        output_files(),
        ensure_partition(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_info=get_list_selector_info(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
    )


certificats_igc()
