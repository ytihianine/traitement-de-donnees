"""MinIO/S3 task utilities using infrastructure handlers."""

import logging
from pathlib import Path
from typing import Any, Mapping

from airflow.sdk import chain, get_current_context, task, task_group
import pandas as pd

from _types.projet import ProjetS3, SelecteurConfig, SelecteurStorageOptions
from infra.file_handling.exceptions import FileHandlerError, FileNotFoundError
from infra.file_handling.factory import create_default_s3_handler
from infra.catalog.iceberg import IcebergCatalog, generate_catalog_properties
from utils.config.dag_params import (
    get_dag_status,
    get_execution_date,
    get_feature_flags,
    get_project_name,
)
from utils.config.tasks import (
    get_list_selecteur_storage_info,
    get_projet_s3_info,
    merge_selecteur_config,
)
from enums.dags import DagStatus, TypeSource
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
    selecteur_options: Mapping[str, SelecteurStorageOptions] | None = None,
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

    # Créer les hooks
    s3_handler = create_default_s3_handler(
        connection_id=connection_id,
    )

    # Get selecteur config
    selecteur_info = get_list_selecteur_storage_info(
        nom_projet=nom_projet, context=context
    )
    selecteur_config = merge_selecteur_config(
        selecteur_info=selecteur_info, options_map=selecteur_options
    )

    # Copier la liste des sources dans le dossier final
    for config in selecteur_config:
        if config.options.write_to_s3 is False:
            logging.info(
                msg=f"write_to_s3 option is set to False for selecteur <{config.selecteur_info.selecteur}>. Skipping copy to S3 ..."  # noqa
            )
            continue

        target_key = f"{config.selecteur_info.s3_key}/{curr_day}/{curr_time}/{config.selecteur_info.filename}"
        src_key = config.selecteur_info.get_full_s3_key()

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
    selecteur_options: Mapping[str, SelecteurStorageOptions],
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    **context: Mapping[str, Any],
) -> None:
    """Delete files from MinIO/S3 storage.

    Args:
        selecteur_options: Mapping of selecteur options
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

    if dag_status == DagStatus.DEV:
        print("Dag status parameter is set to DEV -> skipping this task ...")
        return

    if not s3_enable:
        print(FF_S3_DISABLED_MSG)
        return

    # Créer les hooks
    s3_handler = create_default_s3_handler(
        connection_id=s3_conn_id,
    )

    # Get selecteur config
    selecteur_info = get_list_selecteur_storage_info(
        nom_projet=nom_projet, context=context
    )
    selecteur_config = merge_selecteur_config(
        selecteur_info=selecteur_info, options_map=selecteur_options
    )

    for config in selecteur_config:
        if config.selecteur_info.type_source == TypeSource.FILE:
            s3_key_source = config.selecteur_info.get_full_s3_key()
            try:
                logging.info(msg=f"Deleting source file {s3_key_source}")
                s3_handler.delete_single(file_path=s3_key_source)
                logging.info(msg="Source file deleted successfully")
            except FileHandlerError as e:
                logging.error(
                    msg=f"Failed to delete source file {s3_key_source}: {str(e)}"
                )
                raise

        if config.options.write_to_s3 is True:
            s3_key = config.selecteur_info.get_full_s3_key(with_tmp_segment=True)
            try:
                logging.info(msg=f"Deleting {s3_key} source files")
                s3_handler.delete_single(file_path=s3_key)
                logging.info(msg="Source files deleted successfully")
            except FileHandlerError as e:
                logging.error(msg=f"Failed to delete source files: {str(e)}")
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


@task(map_index_template="{{ task_name }}")
def copy_staging_to_prod(selecteur_config: SelecteurConfig) -> None:
    """Copy Iceberg tables from staging key to prod key"""

    context = get_current_context()
    context["task_name"] = selecteur_config.selecteur_info.selecteur  # type: ignore

    if selecteur_config.selecteur_info.selecteur == "grist_doc":
        logging.info(msg="Grist doc selecteur. Skipping ...")
        return

    if selecteur_config.options.write_to_s3_with_iceberg is False:
        logging.info(
            msg=f"write_to_s3_with_iceberg option is set to False for selecteur <{selecteur_config.selecteur_info.selecteur}>. Skipping import to S3 ..."  # noqa
        )
        return

    # Dag info
    namespace = selecteur_config.selecteur_info.get_iceberg_namespace(with_bucket=False)
    tbl_name = Path(selecteur_config.selecteur_info.filename).stem

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
    catalog.drop_table(table_name=namespace + "." + tbl_name + "_staging", purge=True)


@task_group()
def iceberg_copy_staging_to_prod(
    nom_projet: str | None = None,
    selecteur_options: Mapping[str, SelecteurStorageOptions] | None = None,
    **context,
) -> None:
    """Copy Iceberg tables from staging key to prod key in parallel"""
    if nom_projet is None:
        nom_projet = get_project_name(context=context)

    # Get selecteur config
    selecteur_info = get_list_selecteur_storage_info(
        nom_projet=nom_projet, context=context
    )
    selecteur_config = merge_selecteur_config(
        selecteur_info=selecteur_info, options_map=selecteur_options
    )

    chain(
        copy_staging_to_prod.expand(
            selecteur_config=selecteur_config,
        ),
    )
