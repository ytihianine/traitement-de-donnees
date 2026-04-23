from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from src.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from src._enums.dags import DagStatus
from src._types.dags import DBParams, FeatureFlagsEnable
from src.common_tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    import_file_to_db,
)
from src.common_tasks.grist import download_grist_doc_to_s3
from src.common_tasks.projet import get_selecteur_config
from src.utils.config.dag_params import create_dag_params, create_default_args

from src.common_tasks.validation import validate_dag_parameters
from src.dags.sg.dsci.experimentation_ia.tasks import (
    referentiels,
    repartition,
    suivi_experimentateurs,
    suivi_questionnaire_1,
    suivi_questionnaire_2,
)
from src.dags.sg.dsci.experimentation_ia.config import (
    storage_options,
)

# Variables
nom_projet = "Experimentation IA"


@dag(
    dag_id="experimentation_ia",
    schedule="*/8 8-13,14-19 * * 1-5",
    default_args=create_default_args(),
    max_consecutive_failed_dag_runs=1,
    max_active_runs=1,
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="assistant_ia"),
        feature_flags=FeatureFlagsEnable(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def experimentation_ia_dag() -> None:
    selecteur_configs = get_selecteur_config(storage_options=storage_options)

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
            repartition(),
            suivi_experimentateurs(),
            suivi_questionnaire_1(),
            suivi_questionnaire_2(),
        ],
        create_tmp_tables(storage_options=storage_options, reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(
            storage_options=storage_options,
        ),
        delete_tmp_tables(
            storage_options=storage_options,
        ),
    )


experimentation_ia_dag()
