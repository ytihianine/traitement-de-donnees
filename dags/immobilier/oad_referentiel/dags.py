from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.oad_referentiel.config import dag_id_oad_ref, execution_options, nom_projet_oad_ref
from dags.sg.siep.mmsi.oad_referentiel.tasks import ref_typologie
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
    refresh_views,
    # set_dataset_last_update_date,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback


# Définition du DAG
@dag(
    dag_id=dag_id_oad_ref,
    schedule=None,  # timedelta(seconds=30),
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des référentiels issus de l'OAD.""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet_oad_ref,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False),
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
        bucket_key=DEFAULT_DATASET_CONTEXT_REPO.get_list_source_fichier(nom_projet=nom_projet_oad_ref),
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
        validate_dag_parameters(),
        looking_for_files,
        create_projet_snapshot(nom_projet_parent="Outil aide diagnostic"),
        create_tmp_tables(execution_options=execution_options),
        ref_typologie(),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        refresh_views(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
    )


oad_referentiel()
