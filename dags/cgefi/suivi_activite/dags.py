from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from datetime import timedelta

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from enums.dags import DagStatus
from types.dags import DBParams, FeatureFlags
from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
)
from utils.config.tasks import get_projet_config
from utils.config.dag_params import create_dag_params, create_default_args
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.s3 import del_s3_files

from dags.cgefi.suivi_activite.tasks import (
    referentiels,
    informations_generales,
    processus_4,
    processus_6,
    processus_atpro,
)

nom_projet = "Emploi et formation"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/carte_identite_mef?ref_type=heads"  # noqa
LINK_DOC_DATA = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
)


# Définition du DAG
@dag(
    dag_id="cgefi_suivi_activite",
    schedule="*/5 * * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["CGEFI", "ACTIVITE", "POC", "TABLEAU DE BORD"],
    description="""Pipeline qui récupère les nouvelles données dans Grist
        pour actualiser le tableau de bord de suivi d'activité du CGEFI""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=1, retry_delay=timedelta(seconds=30)),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="cgefi_poc"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def suivi_activite():
    """Task order"""
    chain(
        # projet_config,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci-cgefi",
            doc_id_key="grist_doc_id_cgefi_suivi_activite",
        ),
        [
            referentiels(),
            informations_generales(),
            processus_4(),
            processus_6(),
            processus_atpro(),
        ],
        create_tmp_tables(),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        del_s3_files(),
        delete_tmp_tables(),
    )


suivi_activite()
