from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import MailStatus, create_send_mail_callback
from enums.dags import DagStatus
from _types.dags import DBParams, FeatureFlags
from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
)
from utils.tasks.grist import download_grist_doc_to_s3
from utils.config.tasks import get_list_selector_info
from utils.config.dag_params import create_dag_params, create_default_args

from utils.tasks.validation import validate_dag_parameters
from dags.sg.dsci.accompagnements_dsci.tasks import (
    referentiels,
    bilaterales,
    correspondant,
    mission_innovation,
)

# Variables
nom_projet = "Accompagnements DSCI"


@dag(
    dag_id="accompagnements_dsci",
    schedule="*/8 8-13,14-19 * * 1-5",
    default_args=create_default_args(),
    max_consecutive_failed_dag_runs=1,
    max_active_runs=1,
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="activite_dsci"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def accompagnements_dsci_dag() -> None:

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
        ),
        [referentiels(), bilaterales(), correspondant(), mission_innovation()],
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_info=get_list_selector_info(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        delete_tmp_tables(),
    )


accompagnements_dsci_dag()
