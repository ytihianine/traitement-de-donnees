from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from src.infra.mails.default_smtp import create_send_mail_callback, MailStatus

from src._types.dags import DBParams, FeatureFlags
from src.utils.config.dag_params import create_default_args, create_dag_params
from src.enums.dags import DagStatus
from src.common_tasks.grist import download_grist_doc_to_s3
from src.common_tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    get_projet_snapshot,
)
from src.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from src.common_tasks.projet import get_selecteur_config

from src.common_tasks.validation import validate_dag_parameters
from src.dags.cbcm.ref_service_prescripteur.tasks import (
    grist_source,
    fetch_from_db,
    load_to_grist,
)
from src.dags.cbcm.ref_service_prescripteur.config import selecteur_options

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
    selecteur_configs = get_selecteur_config(selecteur_mapping=selecteur_options)

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc", workspace_id="dsci", doc_id_key="grist_doc_id_cbcm"
        ),
        get_projet_snapshot(nom_projet="Données comptable"),
        grist_source(),
        fetch_from_db(),
        load_to_grist(),
        create_tmp_tables(selecteur_options=selecteur_options, reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(selecteur_options=selecteur_options),
        copy_s3_files(
            selecteur_options=selecteur_options,
        ),
        del_s3_files(
            selecteur_options=selecteur_options,
        ),
        delete_tmp_tables(),
    )


chorus_service_prescripteur()
