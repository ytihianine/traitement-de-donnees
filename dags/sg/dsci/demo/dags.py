from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.demo.config import (
    execution_options,
)
from dags.sg.dsci.demo.tasks import activite, referentiels
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
    ensure_partition,
    update_projet_snapshot_status,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback

# Variables
nom_projet = "Démo traitement données"


@dag(
    dag_id="demo_traitement_donnees",
    schedule="0 8-12,14-18 * * 1-5",
    default_args=create_default_args(),
    max_consecutive_failed_dag_runs=1,
    max_active_runs=1,
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="demo_traitement"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def demo_traitement_dag() -> None:

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        download_grist_doc_to_s3(
            dataset_name="grist_doc",
            workspace_id="dsci",
        ),
        create_projet_snapshot(),
        create_tmp_tables(execution_options=execution_options, reset_id_seq=False),
        [
            referentiels(),
            activite(),
        ],
        ensure_partition(execution_options=execution_options),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(
            execution_options=execution_options,
        ),
        del_s3_files(
            execution_options=execution_options,
        ),
        delete_tmp_tables(execution_options=execution_options),
        update_projet_snapshot_status(),
    )


demo_traitement_dag()
