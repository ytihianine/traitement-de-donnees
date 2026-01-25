from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

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
from utils.config.dag_params import create_default_args, create_dag_params
from utils.tasks.grist import download_grist_doc_to_s3
from dags.sg.sircom.tdb_interne.tasks import (
    validate_params,
    abonnes_visites,
    budget,
    enquetes,
    metiers,
    ressources_humaines,
)


# Mails
nom_projet = "TdB interne - SIRCOM"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/traitement-des-donnees/-/tree/main/dags/sg/sircom/tdb_interne?ref_type=heads"  # noqa
LINK_DOC_DATA = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
)


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
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def tdb_sircom() -> None:
    """Task order"""
    chain(
        validate_params(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_tdb_sircom",
        ),
        [abonnes_visites(), budget(), enquetes(), metiers(), ressources_humaines()],
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        delete_tmp_tables(),
    )


tdb_sircom()
