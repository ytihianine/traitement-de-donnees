from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.carte_identite_mef.config import (
    execution_options,
)
from dags.sg.dsci.carte_identite_mef.tasks import (
    budget,
    effectif,
    plafond,
    taux_agent,
)
from modules.domain.dag.model import DagStatus, DBParams, FeatureFlagsEnable
from modules.infra.airflow.common_tasks.grist import download_grist_doc_to_s3
from modules.infra.airflow.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from modules.infra.airflow.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    update_projet_snapshot_status,
)
from modules.infra.airflow.common_tasks.validation import validate_dag_parameters
from modules.infra.airflow.dag import create_dag_params, create_default_args

nom_projet = "Carte_Identite_MEF"


@dag(
    dag_id="carte_identite_mef",
    schedule="*/8 8-13,14-19 * * 1-5",
    catchup=False,
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="dsci"),
        feature_flags=FeatureFlagsEnable(db=True, mail=True, s3=True, convert_files=False, download_grist_doc=True),
    ),
)
def carte_identite_mef_dag() -> None:
    """Tasks order"""

    chain(
        validate_dag_parameters(),
        download_grist_doc_to_s3(
            dataset_name="grist_doc",
            workspace_id="dsci",
        ),
        create_projet_snapshot(),
        create_tmp_tables(execution_options=execution_options, reset_id_seq=False),
        [effectif(), budget(), taux_agent(), plafond()],
        copy_tmp_table_to_real_table(execution_options=execution_options),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
        update_projet_snapshot_status(),
    )


carte_identite_mef_dag()
