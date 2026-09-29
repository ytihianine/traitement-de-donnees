from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.grist.config import execution_options
from dags.dge.carto_rem.grist.tasks import (
    referentiels,
    source_grist,
)
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.grist import download_grist_doc_to_s3
from modules.infra.airflow.common_tasks.projet import get_projet_datasets_context
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    copy_staging_to_prod,
    del_iceberg_staging_table,
    del_s3_files,
)
from modules.infra.airflow.common_tasks.sql import create_projet_snapshot
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback

# Mails
nom_projet = "Cartographie rémunération - Grist"


# Définition du DAG
@dag(
    dag_id="cartographie_remuneration_grist",
    schedule="*/8 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DGE", "RH"],
    description="""DGE - Cartographie rémunération""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="cartographie_remuneration"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=False, convert_files=False, download_grist_doc=True),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration_grist() -> None:
    """Task order"""
    datasets_context = get_projet_datasets_context()

    chain(
        validate_dag_parameters(),
        create_projet_snapshot(nom_projet="Cartographie rémunération"),
        del_iceberg_staging_table(),
        download_grist_doc_to_s3(dataset_name="grist_doc"),
        [referentiels(), source_grist()],
        copy_staging_to_prod.expand(dataset_context=datasets_context),
        del_iceberg_staging_table(),
        copy_s3_files(
            execution_options=execution_options,
        ),
        del_s3_files(
            execution_options=execution_options,
        ),
    )


cartographie_remuneration_grist()
