from datetime import timedelta
import logging
from typing import Callable
from airflow.sdk import task
from airflow.sdk import Variable
import pandas as pd

from src.infra.file_system.factory import create_file_handler
from src.infra.http_client.adapters import RequestsClient
from src.infra.http_client.config import ClientConfig
from src.infra.grist.client import GristAPI
from src.utils.config.dag_params import get_project_name, should_skip_task
from src._enums.dags import FeatureFlags
from src.utils.config.tasks import get_selecteur_storage_info

from src._enums.filesystem import FileHandlerType
from src.constants import (
    DEFAULT_GRIST_HOST,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_CONN_ID,
    PROXY,
    AGENT,
)
from src.utils.process.dates import convert_grist_date_to_date
from src.utils.process.structures import (
    handle_grist_boolean_columns,
    handle_grist_null_references,
    normalize_grist_dataframe,
)
from src.utils.process.text import normalize_whitespace_columns


@task(
    task_id="download_grist_doc_to_s3",
    retries=5,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,
)
def download_grist_doc_to_s3(
    selecteur: str,
    workspace_id: str,
    grist_host: str = DEFAULT_GRIST_HOST,
    api_token_key: str = "grist_secret_key",
    use_proxy: bool = True,
    **context,
) -> None:
    """Download SQLite from a specific Grist doc to S3"""
    nom_projet = get_project_name(context=context)

    if should_skip_task(context=context, feature_flag=FeatureFlags.DOWNLOAD_GRIST_DOC):
        return

    selecteur_config = get_selecteur_storage_info(
        nom_projet=nom_projet, selecteur=selecteur
    )
    doc_id = selecteur_config.id_source
    dest_tmp_key = selecteur_config.get_full_s3_key(
        with_tmp_segment=True, use_id_source=False
    )

    # Instanciate Grist client
    http_config = ClientConfig()
    if use_proxy:
        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)

    request_client = RequestsClient(config=http_config)

    grist_client = GristAPI(
        http_client=request_client,
        base_url=grist_host,
        workspace_id=workspace_id,
        doc_id=doc_id,
        api_token=Variable.get(key=api_token_key),
    )

    # Hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        connection_id=DEFAULT_S3_CONN_ID,
        bucket=DEFAULT_S3_BUCKET,
    )

    # Get document data from Grist
    grist_response = grist_client.get_doc_sqlite_file()

    # Export sqlite file to S3
    print(f"Exporting file to < {dest_tmp_key} >")
    s3_handler.write(
        file_path=dest_tmp_key,
        content=grist_response,
    )
    print("✅ Export done!")


def generic_grist_processing(
    *,
    df: pd.DataFrame,
    cols_mapping: dict[str, str] | None = None,
    cols_to_keep: list[str] | None = None,
    txt_columns: list[str] | None = None,
    ref_columns: list[str] | None = None,
    date_columns: list[str] | None = None,
    bool_columns: list[str] | None = None,
    custom_fn: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """
    Generic processing for Grist dataframes.
    """
    # Normalize source
    logging.info("Normalizing Grist dataframe")
    df = normalize_grist_dataframe(df=df)

    # Rename columns
    if cols_mapping:
        logging.info(f"Renaming columns: {cols_mapping}")
        df = df.rename(columns=cols_mapping)
    else:
        logging.info("No column renaming mapping provided. Skipping ...")

    # Keep only mandatory columns
    if cols_to_keep:
        logging.info(f"Keeping only mandatory columns: {cols_to_keep}")
        df = df[cols_to_keep]
    else:
        logging.info(
            "No mandatory columns provided. Using all available columns in the dataframe."
        )

    # Normalizing text columns
    if txt_columns:
        logging.info(f"Normalizing text columns to string: {txt_columns}")
        df = normalize_whitespace_columns(df=df, columns=txt_columns)
    else:
        logging.info("No text columns provided. Skipping ...")

    # Convert reference columns to string
    if ref_columns:
        logging.info(f"Converting reference columns to string: {ref_columns}")
        df = handle_grist_null_references(df=df, columns=ref_columns)
    else:
        logging.info("No reference columns provided. Skipping ...")

    # Convert date columns to datetime
    if date_columns:
        logging.info(f"Converting date columns to datetime: {date_columns}")
        df = convert_grist_date_to_date(df=df, columns=date_columns)
    else:
        logging.info("No date columns provided. Skipping ...")

    # Convert boolean columns to boolean
    if bool_columns:
        logging.info(f"Converting boolean columns to boolean: {bool_columns}")
        df = handle_grist_boolean_columns(df=df, columns=bool_columns)
    else:
        logging.info("No boolean columns provided. Skipping ...")

    if custom_fn:
        logging.info(f"Applying custom processing function: {custom_fn.__name__}")
        df = custom_fn(df)
    else:
        logging.info("No custom processing function provided.")

    return df
