from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from src._types.dags import DBParams, FeatureFlagsEnable
from src.inframails.default_smtp import create_send_mail_callback, MailStatus
from src.utils.config.dag_params import create_dag_params, create_default_args
from src._enums.dags import DagStatus
from src.common_tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    create_projet_snapshot,
    get_projet_snapshot,
    import_file_to_db,
)
from src.common_tasks.grist import download_grist_doc_to_s3
from src.constants import DEFAULT_PG_CONFIG_CONN_ID

from src.common_tasks.validation import validate_dag_parameters
from src.common_tasks.projet import get_selecteur_config
from src.common_tasks.s3 import (
    copy_s3_files,
    copy_staging_to_prod,
    del_s3_files,
    del_iceberg_staging_table,
)
from dags.applications.configuration_projets.tasks import (
    process_data,
)
from dags.applications.configuration_projets.config import (
    selecteur_options,
)

nom_projet = "Configuration des projets"


@dag(
    dag_id="configuration_projets",
    schedule="@Daily",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="conf_projets"),
        feature_flags=FeatureFlagsEnable(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=True
        ),
    ),
    on_success_callback=create_send_mail_callback(
        mail_status=MailStatus.SUCCESS,
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def configuration_projets() -> None:
    """Tasks order"""

    selecteur_configs = get_selecteur_config(selecteur_mapping=selecteur_options)

    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
        ),
        create_projet_snapshot(),
        get_projet_snapshot(),
        process_data(),
        delete_tmp_tables(
            selecteur_options=selecteur_options, pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID
        ),
        create_tmp_tables(
            selecteur_options=selecteur_options,
            pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID,
            reset_id_seq=False,
        ),
        import_file_to_db.partial(pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID).expand(
            selecteur_config=selecteur_configs
        ),
        copy_tmp_table_to_real_table(
            selecteur_options=selecteur_options, pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID
        ),
        copy_s3_files(
            selecteur_options=selecteur_options,
        ),
        del_s3_files(
            selecteur_options=selecteur_options,
        ),
        delete_tmp_tables(
            selecteur_options=selecteur_options, pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID
        ),
        copy_staging_to_prod.expand(selecteur_config=selecteur_configs),
        del_iceberg_staging_table(),
    )


configuration_projets()
