from datetime import timedelta

from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from src.infra.mails.default_smtp import create_send_mail_callback, MailStatus

from src._enums.dags import DagStatus
from src._types.dags import DBParams, FeatureFlagsEnable
from src.common_tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    ensure_partition,
    create_projet_snapshot,
    get_projet_snapshot,
)
from src.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from src.common_tasks.projet import get_selecteur_config
from src.utils.config.dag_params import create_default_args, create_dag_params
from src.utils.config.tasks import get_list_source_fichier

from src.common_tasks.validation import validate_dag_parameters
from src.dags.cbcm.donnee_comptable.tasks import (
    source_files,
    datasets_additionnels,
)
from src.dags.cbcm.donnee_comptable.config import (
    selecteur_options,
)

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
    description="Traitement des données comptables issues de Chorus",  # noqa
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="donnee_comptable"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=False,
            s3=False,
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
        bucket_key=get_list_source_fichier(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.START),
    )
    selecteur_configs = get_selecteur_config(selecteur_mapping=selecteur_options)

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        selecteur_configs,
        looking_for_files,
        create_projet_snapshot(nom_projet=nom_projet),
        get_projet_snapshot(nom_projet=nom_projet),
        source_files(),
        datasets_additionnels(),
        create_tmp_tables(selecteur_options=selecteur_options, reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        ensure_partition.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(
            selecteur_options=selecteur_options,
        ),
        copy_s3_files(
            selecteur_options=selecteur_options,
        ),
        del_s3_files(
            selecteur_options=selecteur_options,
        ),
        delete_tmp_tables(),
    )


chorus_donnees_comptables()
