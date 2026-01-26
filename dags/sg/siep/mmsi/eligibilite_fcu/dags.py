from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import create_send_mail_callback, MailStatus
from _types.dags import DBParams, FeatureFlags
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_projet_config
from enums.dags import DagStatus
from enums.database import LoadStrategy
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    ensure_partition,
    get_projet_snapshot,
    import_file_to_db,
)

from utils.tasks.validation import validate_dag_parameters
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.sg.siep.mmsi.eligibilite_fcu.task import (
    get_eligibilite_fcu,
    process_fcu_result,
)


nom_projet = "France Chaleur Urbaine (FCU)"


# Définition du DAG
@dag(
    dag_id="eligibilite_fcu",
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "FCU"],
    description="Récupérer pour chaque bâtiment son éligibilité au réseau Franche Chaleur Urbaine (FCU)",  # noqa
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlags(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def eligibilite_fcu_dag() -> None:

    chain(
        validate_dag_parameters(),
        get_projet_snapshot(nom_projet="Outil aide diagnostic"),
        get_eligibilite_fcu(),
        process_fcu_result(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(load_strategy=LoadStrategy.APPEND),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
    )


eligibilite_fcu_dag()
