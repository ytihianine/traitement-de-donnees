from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.cbcm.ref_service_prescripteur.config import execution_options
from dags.cbcm.ref_service_prescripteur.tasks import (
    fetch_from_db,
    grist_source,
    load_to_grist,
)
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.grist import download_grist_doc_to_s3
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from modules.infra.airflow.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback

# Variables
nom_projet = "Données comptable - référentiel"


# Définition du DAG
@dag(
    dag_id="chorus_service_prescripteur",
    schedule="*/7 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=2,
    catchup=False,
    tags=["CBCM", "DEV", "CHORUS"],
    description="Traitement du référentiel des services prescripteurs (données comptables)",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="donnee_comptable"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def chorus_service_prescripteur() -> None:
    """Task definition"""

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        create_projet_snapshot(nom_projet_parent="Données comptable"),
        download_grist_doc_to_s3(dataset_name="grist_doc", workspace_id="dsci", doc_id_key="grist_doc_id_cbcm"),
        create_tmp_tables(execution_options=execution_options, reset_id_seq=False),
        grist_source(),
        fetch_from_db(),
        load_to_grist(),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(
            execution_options=execution_options,
        ),
        del_s3_files(
            execution_options=execution_options,
        ),
        delete_tmp_tables(),
    )


chorus_service_prescripteur()
