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
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    ensure_partition,
    get_projet_snapshot,
    refresh_views,
    # set_dataset_last_update_date,
)
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import get_list_source_fichier_key, get_list_selector_info

from utils.tasks.validation import validate_dag_parameters
from dags.sg.siep.mmsi.consommation_batiment.tasks import (
    conso_mens_parquet,
    source_files,
    additionnal_files,
)


# Mails
nom_projet = "Consommation des bâtiments"


# Définition du DAG
@dag(
    dag_id="consommation_des_batiments",
    schedule="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "CONSOMMATION"],
    description="Pipeline de traitement des données de consommation des bâtiments. Source des données: OSFI",  # noqa
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=True, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def consommation_des_batiments() -> None:
    """Task definition"""
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

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        looking_for_files,
        get_projet_snapshot(nom_projet="Outil aide diagnostic"),
        conso_mens_parquet(),
        source_files(),
        additionnal_files(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.expand(
            selecteur_info=get_list_selector_info(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(),
        refresh_views(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
        # set_dataset_last_update_date(
        #     dataset_ids=[49, 50, 51, 52, 53, 54],
        # ),
    )


consommation_des_batiments()
