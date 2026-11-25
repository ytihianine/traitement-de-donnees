from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.fichiers.config import config_projet_subpath, execution_options, projet_dge_remuneration
from dags.dge.carto_rem.fichiers.tasks import (
    source_files,
)
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.dbt.tasks import dbt_build_models
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
    looking_for_files,
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


# Définition du DAG
@dag(
    dag_id="cartographie_remuneration",
    schedule="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DGE", "RH"],
    description="""DGE - Cartographie rémunération""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=projet_dge_remuneration,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="cartographie_remuneration"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=True,
            s3=True,
            convert_files=False,
            download_grist_doc=False,
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration() -> None:
    """Task order"""
    chain(
        validate_dag_parameters(),
        looking_for_files(nom_projet=projet_dge_remuneration),
        create_projet_snapshot(),
        create_tmp_tables(
            execution_options=execution_options,
        ),
        source_files(),
        ensure_partition(
            execution_options=execution_options,
        ),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(execution_options=execution_options),
        dbt_build_models(
            project_dir_subpath=config_projet_subpath,
        ),
        update_projet_snapshot_status(execution_options=execution_options),
        del_s3_files(execution_options=execution_options),
        delete_tmp_tables(execution_options=execution_options),
    )


cartographie_remuneration()
