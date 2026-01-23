from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from infra.mails.default_smtp import MailStatus, create_airflow_callback
from enums.dags import DagStatus
from utils.config.dag_params import create_dag_params, create_default_args
from dags.sg.dsci.weather_api.tasks import (
    validate_params,
    extract_data,
    transform_data,
    load_data,
)

# Variables
nom_projet = "Météo"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/traitement-des-donnees/-/tree/dev/dags/sg/dsci/weather_api?ref_type=heads"  # noqa


@dag(
    dag_id="weather_api",
    schedule="*/8 8-13,14-19 * * 1-5",
    default_args=create_default_args(),
    max_consecutive_failed_dag_runs=1,
    max_active_runs=1,
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        prod_schema="activite_meteo",
        lien_pipeline=LINK_DOC_PIPELINE,
        mail_enable=True,
        mail_to=["moussa.sissoko@finances.gouv.fr"],
    ),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def weather_dag() -> None:

    # def flux data
    raw = extract_data()
    transformed = transform_data(raw)
    loaded = load_data(transformed)
    # Ordre tâches
    chain(validate_params(), raw, transformed, loaded)


weather_dag()
