from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.cbcm.donnee_comptable.config import (
    execution_options,
)
from dags.cbcm.donnee_comptable.tasks import (
    datasets_additionnels,
    source_files,
)
from modules.containers import DEFAULT_DATASET_CONTEXT_REPO
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from modules.infra.airflow.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    ensure_partition,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback

# Mails
nom_projet = "Données comptable"


# Définition du DAG
@dag(
    dag_id="chorus_donnees_comptables",
    schedule="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["CBCM", "DEV", "CHORUS"],
    description="Traitement des données comptables issues de Chorus",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="donnee_comptable"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=True,
            s3=True,
            convert_files=False,
            download_grist_doc=False,
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def chorus_donnees_comptables() -> None:
    """Task definition"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=DEFAULT_DATASET_CONTEXT_REPO.get_list_source_fichier(nom_projet=nom_projet),
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
        create_projet_snapshot(nom_projet=nom_projet),
        create_tmp_tables(execution_options=execution_options, reset_id_seq=False),
        source_files(),
        datasets_additionnels(),
        ensure_partition(execution_options=execution_options),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(execution_options=execution_options),
        del_s3_files(execution_options=execution_options),
        delete_tmp_tables(execution_options=execution_options),
    )


chorus_donnees_comptables()
