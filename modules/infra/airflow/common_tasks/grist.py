import logging
from collections.abc import Callable
from datetime import timedelta

import pandas as pd
from airflow.sdk import Variable, task

from modules.constants import (
    AGENT,
    DEFAULT_GRIST_HOST,
    PROXY,
)
from modules.containers import DEFAULT_DAG_REPO, DEFAULT_DATASET_CONTEXT_REPO
from modules.domain.dag.model import FeatureFlags
from modules.domain.dag.repository import DagRepository
from modules.domain.dataset.repository import DatasetContextRepository
from modules.generic_processing.dates import convert_grist_date_to_date
from modules.generic_processing.structures import (
    handle_grist_boolean_columns,
    handle_grist_null_references,
    normalize_grist_dataframe,
)
from modules.generic_processing.text import normalize_whitespace_columns
from modules.infra.airflow.dag import should_skip_task
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler
from modules.infra.grist.client import GristClient
from modules.infra.http_client.adapters import RequestsClient
from modules.infra.http_client.config import ClientConfig


@task(
    task_id="download_grist_doc_to_s3",
    retries=5,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,
)
def download_grist_doc_to_s3(
    dataset_name: str,
    grist_host: str = DEFAULT_GRIST_HOST,
    api_token_key: str = "grist_secret_key",
    use_proxy: bool = True,
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    datasetcontext_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    **context,
) -> None:
    """Download SQLite from a specific Grist doc to S3"""
    if should_skip_task(context=context, feature_flag=FeatureFlags.DOWNLOAD_GRIST_DOC):
        return

    nom_projet = dag_repo.get_project_name(context=context)
    dataset_context = datasetcontext_repo.get(nom_projet=nom_projet, nom_dataset=dataset_name)
    doc_id = dataset_context.storage_info.id_source
    dest_tmp_key = dataset_context.storage_info.get_full_s3_key(with_tmp_segment=True, use_id_source=False)

    if doc_id is None:
        raise ValueError(
            f"doc_id is None for dataset {dataset_name} in project {nom_projet}. Please check the configuration."
        )

    # Instanciate Grist client
    http_config = ClientConfig()
    if use_proxy:
        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)

    request_client = RequestsClient(config=http_config)

    grist_client = GristClient(
        http_client=request_client,
        grist_host=grist_host,
        api_token=Variable.get(key=api_token_key),
    )

    # Hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(),
    )

    # Get document data from Grist
    grist_response = grist_client.download_doc(doc_id=doc_id)

    # Export sqlite file to S3
    print(f"Exporting file to < {dest_tmp_key} >")
    s3_handler.write(
        file_path=dest_tmp_key,
        content=grist_response.content,
    )
    logging.info(msg=f"Export done to {dest_tmp_key}!")


def generic_grist_processing(
    *,
    df: pd.DataFrame,
    cols_mapping: dict[str, str] | None = None,
    cols_to_keep: list[str] | None = None,
    txt_columns: list[str] | None = None,
    ref_columns: list[str] | None = None,
    date_columns: list[str] | None = None,
    bool_columns: list[str] | None = None,
    num_columns: list[str] | None = None,
    custom_fn: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """
    Generic processing for Grist dataframes.
    """
    # Normalize source
    logging.info(msg="Normalizing Grist dataframe")
    df = normalize_grist_dataframe(df=df)

    # Keep only mandatory columns
    if cols_to_keep:
        logging.info(msg=f"Keeping only mandatory columns: {cols_to_keep}")
        df = df.loc[:, cols_to_keep]
    else:
        logging.info(msg="No mandatory columns provided. Using all available columns in the dataframe.")

    # Rename columns
    if cols_mapping:
        logging.info(msg=f"Renaming columns: {cols_mapping}")
        df = df.rename(columns=cols_mapping)
    else:
        logging.info(msg="No column renaming mapping provided. Skipping ...")

    # Normalizing text columns
    if txt_columns:
        logging.info(msg=f"Normalizing text columns to string: {txt_columns}")
        df = normalize_whitespace_columns(df=df, columns=txt_columns)
    else:
        logging.info(msg="No text columns provided. Skipping ...")

    # Convert numeric columns to float
    if num_columns:
        logging.info(msg=f"Converting numeric columns to float: {num_columns}")
        for col in num_columns:
            df[col] = pd.to_numeric(arg=df[col], errors="coerce")
    else:
        logging.info(msg="No numeric columns provided. Skipping ...")

    # Convert date columns to datetime
    if date_columns:
        logging.info(msg=f"Converting date columns to datetime: {date_columns}")
        df = convert_grist_date_to_date(df=df, columns=date_columns)
    else:
        logging.info(msg="No date columns provided. Skipping ...")

    # Convert boolean columns to boolean
    if bool_columns:
        logging.info(msg=f"Converting boolean columns to boolean: {bool_columns}")
        df = handle_grist_boolean_columns(df=df, columns=bool_columns)
    else:
        logging.info(msg="No boolean columns provided. Skipping ...")

    # Convert reference columns to string
    if ref_columns:
        logging.info(msg=f"Converting reference columns to string: {ref_columns}")
        df = handle_grist_null_references(df=df, columns=ref_columns)
    else:
        logging.info(msg="No reference columns provided. Skipping ...")

    if custom_fn:
        logging.info(msg=f"Applying custom processing function: {custom_fn.__name__}")
        df = custom_fn(df)
    else:
        logging.info(msg="No custom processing function provided.")

    return df
