from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from src._enums.dags import DagStatus
from src._types.dags import DBParams, FeatureFlagsEnable
from src.common_tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    import_file_to_db,
)
from src.utils.config.dag_params import create_dag_params, create_default_args
from src.common_tasks.projet import get_selecteur_config

from src.common_tasks.grist import download_grist_doc_to_s3
from src.common_tasks.validation import validate_dag_parameters
from src.dags.sg.dsci.carte_identite_mef.tasks import (
    effectif,
    budget,
    taux_agent,
    plafond,
)
from src.dags.sg.dsci.carte_identite_mef.config import (
    storage_options,
)

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
        feature_flags=FeatureFlagsEnable(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=True
        ),
    ),
)
def carte_identite_mef_dag() -> None:
    """Tasks order"""

    selecteur_configs = get_selecteur_config(selecteur_mapping=storage_options)

    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
        ),
        [effectif(), budget(), taux_agent(), plafond()],
        create_tmp_tables(storage_options=storage_options, reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(
            storage_options=storage_options,
        ),
        delete_tmp_tables(
            storage_options=storage_options,
        ),
    )


carte_identite_mef_dag()
