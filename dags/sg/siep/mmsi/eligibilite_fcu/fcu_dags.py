from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_projet_config
from utils.config.types import LoadStrategy
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    ensure_partition,
    get_projet_snapshot,
    import_file_to_db,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.sg.siep.mmsi.eligibilite_fcu.task import (
    get_eligibilite_fcu,
    process_fcu_result,
)


nom_projet = "France Chaleur Urbaine (FCU)"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/eligibilite_fcu?ref_type=heads"  # noqa
LINK_DOC_DATA = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


# Définition du DAG
@dag(
    dag_id="eligibilite_fcu",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "FCU"],
    description="Récupérer pour chaque bâtiment son éligibilité au réseau Franche Chaleur Urbaine (FCU)",  # noqa
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=1, retry_delay=timedelta(seconds=20)),
    params=create_dag_params(
        nom_projet=nom_projet,
        prod_schema="siep",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def eligibilite_fcu_dag() -> None:

    chain(
        get_projet_snapshot(nom_projet="Outil aide diagnostic"),
        get_eligibilite_fcu(),
        process_fcu_result(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(load_strategy=LoadStrategy.APPEND),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
    )


eligibilite_fcu_dag()
