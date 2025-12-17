from airflow.decorators import dag
from airflow.models.baseoperator import chain

from infra.mails.default_smtp import create_airflow_callback, MailStatus

from utils.config.dag_params import create_dag_params, create_default_args
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    refresh_views,
    # set_dataset_last_update_date,
)

from dags.commun.code_geographique.tasks import code_geographique, geojson, code_iso

nom_projet = "Code géographique"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
LINK_DOC_DATA = "Indéfini"  # noqa


# Définition du DAG
@dag(
    dag_id="informations_geographiques",
    schedule_interval="00 00 7 * *",
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "COMMUN", "DSCI"],
    description="""Récupération des codes géographiques""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        prod_schema="commun",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def informations_geographiques() -> None:
    """Récupération de toutes les données géographiques"""

    """ Hooks """

    # Ordre des tâches
    chain(
        code_geographique(),
        geojson(),
        code_iso(),
        create_tmp_tables(),
        copy_tmp_table_to_real_table(),
        refresh_views(),
    )


informations_geographiques()
