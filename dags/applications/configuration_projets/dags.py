from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from _types.dags import DBParams, FeatureFlags
from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args
from enums.dags import DagStatus
from utils.tasks.projet import get_config_selecteur_info
from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
)
from utils.tasks.grist import download_grist_doc_to_s3
from utils.config.vars import DEFAULT_PG_CONFIG_CONN_ID

from utils.config.tasks import get_list_selector_info, serialize_dataclass

from dags.applications.configuration_projets.tasks import (
    validate_params,
    process_data,
)


nom_projet = "Configuration des projets"
LINK_DOC_PIPELINE = "Non-défini"
LINK_DOC_DONNEE = "Non-défini"


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
        feature_flags=FeatureFlags(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def configuration_projets() -> None:
    """Tasks order"""

    selecteur_s3_db = get_config_selecteur_info()
    chain(
        # validate_params(),
        selecteur_s3_db,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
        ),
        process_data(),
        create_tmp_tables(pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID, reset_id_seq=False),
        import_file_to_db.partial(
            pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID, keep_file_id_col=True
        ).expand(selecteur_info=get_list_selector_info(nom_projet=nom_projet)),
        copy_tmp_table_to_real_table(pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID),
        delete_tmp_tables(pg_conn_id=DEFAULT_PG_CONFIG_CONN_ID),
    )


configuration_projets()
