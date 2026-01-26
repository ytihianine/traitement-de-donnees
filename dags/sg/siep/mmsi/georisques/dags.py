from airflow.sdk import dag
from airflow.sdk.bases.operator import chain


from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from enums.dags import DagStatus
from enums.database import LoadStrategy
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    ensure_partition,
    get_projet_snapshot,
    import_file_to_db,
)
from utils.config.tasks import get_projet_config

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from dags.sg.siep.mmsi.georisques.task import georisques_group

# Mails
nom_projet = "Géorisques"


# Définition du DAG
@dag(
    dag_id="georisques_batiments",
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "GEORISQUES"],
    description="Pipeline qui check pour chaque bâtiment les géorisques associés",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def bien_georisques() -> None:
    """Task order"""
    chain(
        get_projet_snapshot(nom_projet="Outil aide diagnostic"),
        georisques_group(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(load_strategy=LoadStrategy.APPEND),
        copy_s3_files(),
        del_s3_files(),
    )


bien_georisques()
