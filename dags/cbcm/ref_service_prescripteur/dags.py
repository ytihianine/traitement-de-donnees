from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_send_mail_callback, MailStatus

from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_default_args, create_dag_params
from utils.config.tasks import get_list_selector_info
from enums.dags import DagStatus
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    LoadStrategy,
    refresh_views,
    # set_dataset_last_update_date,
)
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from utils.tasks.validation import validate_dag_parameters
from dags.cbcm.ref_service_prescripteur.tasks import grist_source, fetch_from_db


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
    description="Traitement du référentiel des services prescripteurs (données comptables)",  # noqa
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="donnee_comptable"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True
        ),
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
        download_grist_doc_to_s3(
            selecteur="grist_doc", workspace_id="dsci", doc_id_key="grist_doc_id_cbcm"
        ),
        grist_source(),
        fetch_from_db(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_info=get_list_selector_info(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(
            load_strategy=LoadStrategy.FULL_LOAD,
        ),
        refresh_views(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
        # set_dataset_last_update_date(
        #     dataset_ids=[49, 50, 51, 52, 53, 54],
        # ),
    )


chorus_service_prescripteur()
