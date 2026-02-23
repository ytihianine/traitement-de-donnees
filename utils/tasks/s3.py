"""MinIO/S3 task utilities using infrastructure handlers."""

import logging
from typing import Any, Mapping
from _types.projet import ProjetS3, SelecteurS3

from airflow.sdk import task
import pandas as pd

from infra.file_handling.exceptions import FileHandlerError, FileNotFoundError
from infra.file_handling.factory import create_file_handler
from infra.catalog.iceberg import IcebergCatalog, generate_catalog_properties
from utils.config.dag_params import (
    get_dag_status,
    get_db_info,
    get_execution_date,
    get_feature_flags,
    get_project_name,
)
from utils.config.tasks import (
    get_projet_s3_info,
    get_projet_selecteur_s3,
    get_list_source_fichier,
)
from enums.dags import DagStatus
from enums.filesystem import FileHandlerType, IcebergTableStatus
from utils.config.vars import (
    DEFAULT_POLARIS_HOST,
    DEFAULT_S3_CONN_ID,
    FF_S3_DISABLED_MSG,
)


@task
def copy_s3_files(
    projet_s3: ProjetS3 | None = None,
    connection_id: str = DEFAULT_S3_CONN_ID,
    **context: Mapping[str, Any],
) -> None:
    """Copy files to S3 storage.

    Args:
        bucket: Target S3 bucket
        connection_id: S3 connection ID
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    # Récupérer les info du dag
    dag_status = get_dag_status(context=context)
    s3_enable = get_feature_flags(context=context).s3
    nom_projet = get_project_name(context=context)
    execution_date = get_execution_date(context=context, use_tz=False)
    curr_day = execution_date.strftime(format="%Y%m%d")
    curr_time = execution_date.strftime(format="%Hh%M")

    if projet_s3 is None:
        projet_s3 = get_projet_s3_info(nom_projet=nom_projet)

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    if not s3_enable:
        print(FF_S3_DISABLED_MSG)
        return

    # Créer les variables
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        connection_id=connection_id,
        bucket=projet_s3.bucket,
    )

    # Récupérer la liste des key s3 pour tous les sélecteurs
    projet_selecteur_s3 = get_projet_selecteur_s3(nom_projet=nom_projet)

    # Copier la liste des sources dans le dossier final
    for selecteur_s3 in projet_selecteur_s3:
        target_key = (
            f"{selecteur_s3.s3_key}/{curr_day}/{curr_time}/{selecteur_s3.filename}"
        )
        src_key = selecteur_s3.filepath_tmp_s3

        try:
            # Copy file
            logging.info(msg=f"Copying {src_key} to {target_key}")
            s3_handler.copy(source=src_key, destination=target_key)
            logging.info(msg="Copy successful")

        except (FileHandlerError, FileNotFoundError) as e:
            logging.error(msg=f"Failed to copy {src_key} to {target_key}: {str(e)}")
            raise FileHandlerError(f"Failed to copy file: {e}") from e
        except Exception as e:
            logging.error(
                msg=f"Unexpected error copying {src_key} to {target_key}: {str(e)}"
            )
            raise


@task
def del_s3_files(
    projet_s3: ProjetS3 | None = None,
    keys_to_delete: list[str] | None = None,
    connection_id: str = DEFAULT_S3_CONN_ID,
    **context: Mapping[str, Any],
) -> None:
    """Delete files from MinIO/S3 storage.

    Args:
        bucket: Target S3/MinIO bucket
        keys_to_delete: Optional list of keys to delete
        connection_id: S3 connection ID
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    # Récupérer les info du dag
    dag_status = get_dag_status(context=context)
    s3_enable = get_feature_flags(context=context).s3
    nom_projet = get_project_name(context=context)

    if projet_s3 is None:
        projet_s3 = get_projet_s3_info(nom_projet=nom_projet)

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    if not s3_enable:
        print(FF_S3_DISABLED_MSG)
        return

    # Créer les variables
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        connection_id=connection_id,
        bucket=projet_s3.bucket,
    )

    # Récupérer les sources
    projet_sources = get_list_source_fichier(nom_projet=nom_projet)

    # Récupérer les key s3 des sélecteurs
    projet_selecteur_s3 = get_projet_selecteur_s3(nom_projet=nom_projet)

    # Supprimer les sources
    if projet_sources:
        try:
            logging.info(msg=f"Deleting {len(projet_sources)} source files")
            for source in projet_sources:
                s3_handler.delete_single(file_path=source.filepath_source_s3)
            logging.info(msg="Source files deleted successfully")
        except FileHandlerError as e:
            logging.error(msg=f"Failed to delete source files: {str(e)}")
            raise

    # Supprimer les key S3
    if projet_selecteur_s3:
        try:
            logging.info(msg=f"Deleting {len(projet_selecteur_s3)} temporary files")
            for selecteur_s3 in projet_selecteur_s3:
                s3_handler.delete_single(file_path=selecteur_s3.filepath_tmp_s3)
            logging.info(msg="Temporary files deleted successfully")
        except FileHandlerError as e:
            logging.error(msg=f"Failed to delete temporary files: {str(e)}")
            raise


def write_to_s3(
    catalog: IcebergCatalog,
    df: pd.DataFrame,
    table_status: IcebergTableStatus,
    namespace: str,
    table_name: str,
) -> None:
    # Create namespace
    catalog.create_namespace(namespace=namespace)

    # load data to table
    if table_status == IcebergTableStatus.STAGING:
        table_name = table_name + "_staging"

    table_name = namespace + "." + table_name
    catalog.write_table(table_name=table_name, df=df)


@task
def copy_staging_to_prod(selecteur_info: SelecteurS3, **context) -> None:
    """Copy Iceberg tables from staging key to prod key"""

    if selecteur_info.selecteur == "grist_doc":
        logging.info(msg="Grist doc selecteur. Skipping ...")
        return

    # Dag info
    db_schema = get_db_info(context=context).prod_schema
    key_split = selecteur_info.filepath_s3.split(sep=".")[0].split(sep="/")
    tbl_name = key_split.pop(-1)
    namespace = ".".join([db_schema] + key_split)
    # Get catalog
    properties = generate_catalog_properties(
        uri=DEFAULT_POLARIS_HOST,
    )
    catalog = IcebergCatalog(name="data_store", properties=properties)

    # Read staging table
    df = catalog.read_table(table_name=namespace + "." + tbl_name + "_staging")

    # Write prod table
    write_to_s3(
        catalog=catalog,
        df=df,
        table_status=IcebergTableStatus.PROD,
        namespace=namespace,
        table_name=tbl_name,
    )
