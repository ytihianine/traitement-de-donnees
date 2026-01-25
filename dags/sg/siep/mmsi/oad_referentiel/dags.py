from datetime import timedelta
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from enums.dags import DagStatus
from utils.tasks.sql import (
    LoadStrategy,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    get_projet_snapshot,
    import_file_to_db,
    delete_tmp_tables,
    refresh_views,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import (
    get_s3_keys_source,
    get_projet_config,
)

from dags.sg.siep.mmsi.oad_referentiel.tasks import validate_params, bien_typologie


# Mails
nom_projet = "Outil aide diagnostic - référentiel"


# Définition du DAG
@dag(
    dag_id="outil_aide_diagnostic_referentiel",
    schedule=None,  # timedelta(seconds=30),
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des référentiels issus de l'OAD.""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def oad_referentiel() -> None:
    """Task definition"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),  # timedelta(minutes=1),
        timeout=timedelta(minutes=1),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(
            mail_status=MailStatus.START,
        ),
    )

    """ Task order """
    chain(
        validate_params(),
        looking_for_files,
        get_projet_snapshot(nom_projet="Outil aide diagnostic"),
        bien_typologie(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(load_strategy=LoadStrategy.APPEND),
        refresh_views(),
        copy_s3_files(
            bucket="dsci",
        ),
        del_s3_files(
            bucket="dsci",
        ),
        delete_tmp_tables(),
    )


oad_referentiel()
