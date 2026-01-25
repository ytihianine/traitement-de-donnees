from datetime import timedelta
from airflow.sdk import dag, task_group
from airflow.sdk.bases.operator import chain

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.default_smtp import MailStatus, create_send_mail_callback
from types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_s3_keys_source, get_projet_config
from enums.dags import DagStatus
from utils.tasks.sql import (
    LoadStrategy,
    create_tmp_tables,
    ensure_partition,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    delete_tmp_tables,
    create_projet_snapshot,
    get_projet_snapshot,
    refresh_views,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.sg.siep.mmsi.oad.caracteristiques.tasks import (
    validate_params,
    oad_carac_to_parquet,
    tasks_oad_caracteristiques,
)
from dags.sg.siep.mmsi.oad.indicateurs.tasks import (
    oad_indic_to_parquet,
    tasks_oad_indicateurs,
)


# Mails
nom_projet = "Outil aide diagnostic"


# Définition du DAG
@dag(
    dag_id="outil_aide_diagnostic",
    schedule="*/15 6-22 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des données de l'immobilier. Base""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlags(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def oad() -> None:
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
        on_success_callback=create_send_mail_callback(
            mail_status=MailStatus.START,
        ),
    )

    @task_group
    def convert_file_to_parquet() -> None:
        chain(
            [
                oad_carac_to_parquet(),
                oad_indic_to_parquet(),
            ]
        )

    end_task = EmptyOperator(
        task_id="end_task",
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
    )

    # Ordre des tâches
    chain(
        validate_params(),
        looking_for_files,
        create_projet_snapshot(),
        get_projet_snapshot(),
        # convert_file_to_parquet(),
        tasks_oad_caracteristiques(),
        tasks_oad_indicateurs(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(
            load_strategy=LoadStrategy.APPEND,
        ),
        refresh_views(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
        end_task,
    )


oad()
