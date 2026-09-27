from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.sircom.tdb_interne.config import (
    execution_options,
)
from dags.sg.sircom.tdb_interne.tasks import (
    abonnes_visites,
    budget,
    enquetes,
    metiers,
    ressources_humaines,
)
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.grist import download_grist_doc_to_s3
from modules.infra.airflow.common_tasks.projet import get_projet_datasets_context
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from modules.infra.airflow.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    update_projet_snapshot_status,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback

# Mails
nom_projet = "TdB interne - SIRCOM"


# Définition du DAG
@dag(
    dag_id="tdb_sircom",
    schedule="*/15 8-13,14-19 * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIRCOM", "PRODUCTION", "TABLEAU DE BORD"],
    description="""Pipeline qui scanne les nouvelles données dans Grist
        pour actualiser le tableau de bord du SIRCOM""",
    max_consecutive_failed_dag_runs=2,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="sircom"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def tdb_sircom() -> None:
    """Task order"""
    datasets_context = get_projet_datasets_context(execution_options=execution_options)

    chain(
        validate_dag_parameters(),
        datasets_context,
        download_grist_doc_to_s3(
            dataset_name="grist_doc",
            workspace_id="dsci",
        ),
        create_projet_snapshot(),
        create_tmp_tables(execution_options=execution_options, reset_id_seq=False),
        abonnes_visites(),
        budget(),
        enquetes(),
        metiers(),
        ressources_humaines(),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(execution_options=execution_options),
        del_s3_files(execution_options=execution_options),
        delete_tmp_tables(execution_options=execution_options),
        update_projet_snapshot_status(),
    )


tdb_sircom()
