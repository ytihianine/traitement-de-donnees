from airflow.sdk import dag
from airflow.sdk.bases.operator import chain

from _types.dags import FeatureFlags
from infra.mails.default_smtp import MailStatus, create_send_mail_callback
from enums.dags import DagStatus
from utils.config.dag_params import create_dag_params, create_default_args

from utils.tasks.validation import validate_dag_parameters
from dags.sg.dsci.weather_api.tasks import (
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
        db_params=None,
        feature_flags=FeatureFlags(
            db=False,
            mail=False,
            s3=False,
            convert_files=False,
            download_grist_doc=False,
        ),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def weather_dag() -> None:

    # def flux data
    raw = extract_data()
    transformed = transform_data(raw)
    loaded = load_data(transformed)
    # Ordre tâches
    chain(validate_dag_parameters(), raw, transformed, loaded)


weather_dag()
