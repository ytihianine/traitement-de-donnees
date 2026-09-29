from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.eligibilite_fcu.config import dag_id_fcu, execution_options, nom_projet_fcu
from dags.sg.siep.mmsi.eligibilite_fcu.task import (
    bien_localisation,
    process_fcu_result,
)
from dags.sg.siep.mmsi.oad.config import nom_projet_oad
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.projet import get_projet_datasets_context
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from modules.infra.airflow.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    ensure_partition,
    update_projet_snapshot_status,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback


# Définition du DAG
@dag(
    dag_id=dag_id_fcu,
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "FCU"],
    description="Récupérer pour chaque bâtiment son éligibilité au réseau Franche Chaleur Urbaine (FCU)",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet_fcu,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlagsEnable(db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False),
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def eligibilite_fcu_dag() -> None:

    datasets_context = get_projet_datasets_context(execution_options=execution_options)

    chain(
        validate_dag_parameters(),
        create_projet_snapshot(nom_projet_parent=nom_projet_oad),
        create_tmp_tables(execution_options=execution_options, reset_id_seq=False),
        bien_localisation(),
        process_fcu_result(),
        ensure_partition.expand(dataset_context=datasets_context),
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(),
        del_s3_files(execution_options=execution_options),
        update_projet_snapshot_status(),
        delete_tmp_tables(execution_options=execution_options),
    )


eligibilite_fcu_dag()
