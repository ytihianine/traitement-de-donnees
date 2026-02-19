""" Répertorie les variables communes à toutes les pipelines """

import os
import sys
import pytz
from functools import lru_cache
import logging


@lru_cache(maxsize=1)
def get_root_folder() -> str:
    """Get root folder based on environment."""
    base_folder = os.getenv("AIRFLOW_HOME")
    if base_folder is None:
        return "/home/onyxia/work/airflow-demo"
    return os.path.join(base_folder, "dags", "repo")


# It doesn't work
custom_logger = logging.Logger(name=__name__, level=logging.DEBUG)
handler = logging.StreamHandler(
    stream=sys.stdout
)  # Handler pour afficher les logs dans la console
formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(fmt=formatter)
custom_logger.addHandler(hdlr=handler)

NO_PROCESS_MSG = "No complementary actions needed ! Skipping ..."

ENV_VAR = os.environ.copy()

""" Configuration du proxy """
PROXY = "172.16.0.53:3128"
AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"
)

""" TimeZone """
paris_tz = pytz.timezone(zone="Europe/Paris")

""" MinIO """
TMP_KEY = "tmp"

# DEFAULT VARIABLES
DEFAULT_SMTP_CONN_ID = "smtp_nubonyxia"
DEFAULT_MAIL_CC = ["labo-data@finances.gouv.fr", "yanis.tihianine@finances.gouv.fr"]
DEFAULT_TMP_SCHEMA = "temporaire"
DEFAULT_PG_DATA_CONN_ID = "db_data_store"
DEFAULT_PG_CONFIG_CONN_ID = "db_depose_fichier"
DEFAULT_S3_CONN_ID = "minio_bucket_dsci"
DEFAULT_S3_BUCKET = "dsci"

# Catalog POLARIS
DEFAULT_POLARIS_HOST = "https://polaris-catalog.lab.incubateur.finances.rie.gouv.fr"

# Grist
DEFAULT_GRIST_HOST = "https://grist.numerique.gouv.fr"

# Feature flags messages
FF_MAIL_DISABLED_MSG = "Mail feature flag is disabled ! Skipping mail sending ..."
FF_S3_DISABLED_MSG = "S3 feature flag is disabled ! Skipping S3 operations ..."
FF_DB_DISABLED_MSG = (
    "Database feature flag is disabled ! Skipping database operations ..."
)
FF_CONVERT_DISABLED_MSG = "File conversion to parquet feature flag is disabled ! Skipping File conversion operations ..."  # noqa
FF_DOWNLOAD_GRIST_DOC_DISABLED_MSG = (
    "Download Grist Doc feature flag is disabled ! Skipping Grist document download ..."
)
