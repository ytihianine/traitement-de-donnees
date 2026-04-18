from datetime import timedelta
from airflow.sdk import task
from airflow.sdk import Variable

from src.infra.file_handling.factory import create_file_handler
from src.infra.http_client.adapters import RequestsClient
from src.infra.http_client.config import ClientConfig
from src.infra.grist.client import GristAPI
from src.utils.config.dag_params import get_feature_flags, get_project_name
from src.utils.config.tasks import get_selecteur_storage_info

from src.enums.filesystem import FileHandlerType
from src.constants import (
    DEFAULT_GRIST_HOST,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_CONN_ID,
    FF_DOWNLOAD_GRIST_DOC_DISABLED_MSG,
    PROXY,
    AGENT,
)


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
    grist_enable = get_feature_flags(context=context).download_grist_doc

    if not grist_enable:
        print(FF_DOWNLOAD_GRIST_DOC_DISABLED_MSG)
        return

    selecteur_config = get_selecteur_storage_info(
        nom_projet=nom_projet, selecteur=selecteur
    )
    doc_id = selecteur_config.id_source
    dest_tmp_key = selecteur_config.get_full_s3_key(
        with_tmp_segment=True, use_id_source=False
    )

    # Instanciate Grist client
    if use_proxy:
        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
        request_client = RequestsClient(config=http_config)
    else:
        http_config = ClientConfig()
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
