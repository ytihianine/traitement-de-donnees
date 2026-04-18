from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from src.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from src.enums.dags import DagStatus
from src._types.dags import DBParams, FeatureFlags
from src.utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    import_file_to_db,
)
from src.utils.tasks.grist import download_grist_doc_to_s3
from src.utils.tasks.projet import get_selecteur_config
from src.utils.config.dag_params import create_dag_params, create_default_args

from src.utils.tasks.validation import validate_dag_parameters
from src.dags.sg.dsci.accompagnements_dsci.tasks import (
    referentiels,
    bilaterales,
    correspondant,
    mission_innovation,
    dsci,
    conseil_interne,
)
from src.dags.sg.dsci.accompagnements_dsci.config import (
    selecteur_options,
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
    selecteur_configs = get_selecteur_config(selecteur_mapping=selecteur_options)

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
        ),
        [
            referentiels(),
            bilaterales(),
            correspondant(),
            mission_innovation(),
            dsci(),
            conseil_interne(),
        ],
        create_tmp_tables(selecteur_options=selecteur_options, reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(
            selecteur_options=selecteur_options,
            merge_delete=True,
        ),
        delete_tmp_tables(
            selecteur_options=selecteur_options,
        ),
    )


accompagnements_dsci_dag()
