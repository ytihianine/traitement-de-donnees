from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from enums.dags import DagStatus
from utils.tasks.grist import download_grist_doc_to_s3

from utils.tasks.sql import get_projet_snapshot
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
    iceberg_copy_staging_to_prod,
    import_files_to_iceberg,
    del_iceberg_staging_table,
)

from utils.tasks.validation import validate_dag_parameters
from dags.dge.carto_rem.grist.tasks import (
    referentiels,
    source_grist,
    # load_to_grist,
)
from dags.dge.carto_rem.grist.config import selecteur_options

# Mails
nom_projet = "Cartographie rémunération - Grist"


# Définition du DAG
@dag(
    dag_id="cartographie_remuneration_grist",
    schedule="*/8 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DGE", "RH"],
    description="""DGE - Cartographie rémunération""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="cartographie_remuneration"),
        feature_flags=FeatureFlags(
            db=True, mail=False, s3=False, convert_files=False, download_grist_doc=True
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration_grist() -> None:
    """Task order"""
    chain(
        validate_dag_parameters(),
        get_projet_snapshot(nom_projet="Cartographie rémunération"),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci-dge",
            doc_id_key="grist_doc_id_carto_rem",
        ),
        [referentiels(), source_grist()],
        # load_to_grist(),
        import_files_to_iceberg(
            nom_projet=nom_projet,
            selecteur_options=selecteur_options,
        ),
        iceberg_copy_staging_to_prod(
            nom_projet=nom_projet,
            selecteur_options=selecteur_options,
        ),
        del_iceberg_staging_table(),
        copy_s3_files(
            selecteur_options=selecteur_options,
        ),
        del_s3_files(
            selecteur_options=selecteur_options,
        ),
    )


cartographie_remuneration_grist()
