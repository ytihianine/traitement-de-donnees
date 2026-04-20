from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from src.inframails.default_smtp import create_send_mail_callback, MailStatus
from src._enums.dags import DagStatus
from src._types.dags import DBParams, FeatureFlagsEnable
from src.common_tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    create_projet_snapshot,
    get_projet_snapshot,
)
from src.utils.config.dag_params import create_default_args, create_dag_params

from src.common_tasks.validation import validate_dag_parameters
from src.common_tasks.grist import download_grist_doc_to_s3
from src.common_tasks.projet import get_selecteur_config
from dags.sg.sircom.tdb_interne.tasks import (
    abonnes_visites,
    budget,
    enquetes,
    metiers,
    ressources_humaines,
)
from dags.sg.sircom.tdb_interne.config import (
    selecteur_options,
)

# Mails
nom_projet = "TdB interne - SIRCOM"


# Définition du DAG
@dag(
    dag_id="tdb_sircom",
    schedule="*/8 8-13,14-19 * * 1-5",
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
        feature_flags=FeatureFlagsEnable(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=True
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def tdb_sircom() -> None:
    """Task order"""
    selecteur_configs = get_selecteur_config(selecteur_mapping=selecteur_options)

    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_tdb_sircom",
        ),
        create_projet_snapshot(),
        get_projet_snapshot(),
        abonnes_visites(),
        budget(),
        enquetes(),
        metiers(),
        ressources_humaines(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(),
        delete_tmp_tables(),
    )


tdb_sircom()
