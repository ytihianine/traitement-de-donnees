from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from enums.dags import DagStatus
from _types.dags import DBParams, FeatureFlags
from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
)
from utils.config.tasks import get_list_selector_info
from utils.config.dag_params import create_dag_params, create_default_args

from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.validation import validate_dag_parameters
from dags.sg.dsci.carte_identite_mef.tasks import (
    effectif,
    budget,
    taux_agent,
    plafond,
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
        feature_flags=FeatureFlags(
            db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False
        ),
    ),
)
def carte_identite_mef_dag() -> None:
    """Tasks order"""
    chain(
        validate_dag_parameters(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
        ),
        [effectif(), budget(), taux_agent(), plafond()],
        create_tmp_tables(),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_info=get_list_selector_info(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        delete_tmp_tables(),
    )


carte_identite_mef_dag()
