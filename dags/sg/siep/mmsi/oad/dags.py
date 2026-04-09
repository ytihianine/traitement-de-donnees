from datetime import timedelta
from airflow.sdk import dag, task_group
from airflow.sdk.bases.operator import chain

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.default_smtp import MailStatus, create_send_mail_callback
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_list_source_fichier
from enums.dags import DagStatus
from utils.tasks.sql import (
    create_tmp_tables,
    ensure_partition,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    create_projet_snapshot,
    get_projet_snapshot,
    import_file_to_db,
    refresh_views,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.tasks.projet import get_selecteur_config

from utils.tasks.validation import validate_dag_parameters
from dags.sg.siep.mmsi.oad.caracteristiques.tasks import (
    oad_carac_to_parquet,
    tasks_oad_caracteristiques,
)
from dags.sg.siep.mmsi.oad.indicateurs.tasks import (
    oad_indic_to_parquet,
    tasks_oad_indicateurs,
)
from dags.sg.siep.mmsi.oad.config import selecteur_options

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
            db=False, mail=False, s3=False, convert_files=True, download_grist_doc=False
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
        bucket_key=get_list_source_fichier(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(
            mail_status=MailStatus.START,
        ),
    )

    selecteur_configs = get_selecteur_config(selecteur_mapping=selecteur_options)

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
        validate_dag_parameters(),
        looking_for_files,
        create_projet_snapshot(),
        get_projet_snapshot(),
        convert_file_to_parquet(),
        tasks_oad_caracteristiques(),
        tasks_oad_indicateurs(),
        create_tmp_tables(selecteur_options=selecteur_options),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        ensure_partition(selecteur_options=selecteur_options),
        copy_tmp_table_to_real_table(selecteur_options=selecteur_options),
        refresh_views(),
        copy_s3_files(selecteur_options=selecteur_options),
        del_s3_files(selecteur_options=selecteur_options),
        delete_tmp_tables(selecteur_options=selecteur_options),
        end_task,
    )


oad()
