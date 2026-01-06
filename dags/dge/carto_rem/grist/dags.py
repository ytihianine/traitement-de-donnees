from airflow.decorators import dag
from airflow.models.baseoperator import chain

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.types import DagStatus
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    delete_tmp_tables,
    refresh_views,
    LoadStrategy,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import get_projet_config

from dags.dge.carto_rem.grist.tasks import (
    validate_params,
    referentiels,
    source_grist,
    get_db_data,
    datasets_additionnels,
)


# Mails
nom_projet = "Cartographie rémunération - Grist"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
LINK_DOC_DATA = "To define"  # noqa


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
        dag_status=DagStatus.DEV.value,
        prod_schema="cartographie_remuneration",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration_grist() -> None:
    """Task order"""
    chain(
        validate_params(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_carto_rem",
        ),
        [referentiels(), source_grist()],
        get_db_data(),
        datasets_additionnels(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(load_strategy=LoadStrategy.FULL_LOAD),
        refresh_views(),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
        delete_tmp_tables(),
    )


cartographie_remuneration_grist()
