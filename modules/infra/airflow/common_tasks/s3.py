"""MinIO/S3 task utilities using infrastructure handlers."""

import logging
from collections.abc import Mapping
from typing import Any

from airflow.sdk import get_current_context, task

from modules.constants import (
    DEFAULT_POLARIS_CATALOG,
    DEFAULT_POLARIS_HOST,
)
from modules.containers import DEFAULT_DAG_REPO, DEFAULT_DATASET_CONTEXT_REPO
from modules.domain.dag.model import FeatureFlags
from modules.domain.dag.repository import DagRepository
from modules.domain.dataset.model import DatasetContext, TypeLocation
from modules.domain.dataset.repository import DatasetContextRepository
from modules.infra.airflow.dag import should_skip_task
from modules.infra.catalog.iceberg import IcebergCatalog, IcebergTableStatus, generate_catalog_properties
from modules.infra.file_system.dataset_location import (
    parse_iceberg_namespace,
    parse_iceberg_table,
    parse_s3_filename,
    parse_s3_prefix,
)
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@task
def copy_s3_files(
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    **context: Mapping[str, Any],
) -> None:
    """Copy files from one place to another in S3 storage.

    Args:
        datasets: Mapping of selecteur options
        connection_id: S3 connection ID (from execution options)
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    # Récupérer les info du dag
    if should_skip_task(context=context, feature_flag=FeatureFlags.S3):
        return

    nom_projet = dag_repo.get_project_name(context=context)
    execution_date = dag_repo.get_execution_date(context=context, use_tz=False)
    curr_day = execution_date.strftime(format="%Y%m%d")
    curr_time = execution_date.strftime(format="%Hh%M")

    datasets_context = dataset_context_repo.get_list(nom_projet=nom_projet)
    # Copier la liste des sources dans le dossier final
    for dataset_context in datasets_context:
        logging.info(msg=f"Processing dataset {dataset_context.dataset.name}")

        tmp_loc = dataset_context.tmp_location
        if tmp_loc.type_location != TypeLocation.S3_FILE:
            logging.info(msg="Temporary location is not an S3 file ... skipping")
            continue

        dest_loc = dataset_context.dest_location
        if dest_loc.type_location != TypeLocation.S3_FILE:
            logging.info(msg="Destination location is not an S3 file ... skipping")
            continue

        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(connection_id=tmp_loc.validate_conn_id),
        )

        prefix_dest_key = parse_s3_prefix(location=dest_loc.validate_location)
        s3_filename = parse_s3_filename(location=dest_loc.validate_location)
        dest_key = f"{prefix_dest_key}/{curr_day}/{curr_time}/{s3_filename}"
        # Copy tmp file if exists
        logging.info(msg=f"Copying {tmp_loc.validate_location} to {dest_key}")
        s3_handler.copy(source=tmp_loc.validate_location, destination=dest_key)
        logging.info(msg="Copy successful")


@task
def del_s3_files(
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    **context: Mapping[str, Any],
) -> None:
    """Delete files from MinIO/S3 storage for the given project.

    Args:
        dataset_context_repo: DatasetContextRepository instance to fetch dataset context information
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    # Récupérer les info du dag
    if should_skip_task(context=context, feature_flag=FeatureFlags.S3):
        return

    nom_projet = dag_repo.get_project_name(context=context)
    datasets_context = dataset_context_repo.get_list(nom_projet=nom_projet)
    for dataset_context in datasets_context:
        # Delete src files
        src_loc = dataset_context.src_location
        logging.info(msg=f"{dataset_context.dataset.name}")
        if src_loc.type_location != TypeLocation.S3_FILE:
            logging.info(
                msg=f"Source location for dataset {dataset_context.dataset.name} is not an S3 file ... skipping"
            )
            continue

        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(connection_id=src_loc.validate_conn_id),
        )
        logging.info(msg=f"Deleting source file {src_loc.validate_location}")
        s3_handler.delete_single(file_path=src_loc.validate_location)
        logging.info(msg="Source file deleted successfully")

        # Delete tmp files
        tmp_loc = dataset_context.tmp_location
        logging.info(msg=f"{dataset_context.dataset.name}")
        if tmp_loc.type_location != TypeLocation.S3_FILE:
            logging.info(
                msg=f"Temporary location for dataset {dataset_context.dataset.name} is not an S3 file ... skipping"
            )
            continue

        if src_loc.validate_conn_id != tmp_loc.validate_conn_id:
            s3_handler = create_file_handler(
                handler_type=FileHandlerType.S3,
                config=FSConfig(connection_id=tmp_loc.validate_conn_id),
            )

        logging.info(msg=f"Deleting {tmp_loc.validate_location} temporary files")
        s3_handler.delete_single(file_path=tmp_loc.validate_location)
        logging.info(msg="Temporary files deleted successfully")


@task
def del_iceberg_staging_table(
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    dataset_context_repo: DatasetContextRepository = DEFAULT_DATASET_CONTEXT_REPO,
    catalog_name: str = DEFAULT_POLARIS_CATALOG,
    **context: Mapping[str, Any],
) -> None:
    """Delete Iceberg staging table."""
    # Get catalog
    properties = generate_catalog_properties(
        uri=DEFAULT_POLARIS_HOST,
    )
    catalog = IcebergCatalog(name=catalog_name, properties=properties)

    nom_projet = dag_repo.get_project_name(context=context)
    datasets_context = dataset_context_repo.get_list(nom_projet=nom_projet)
    for dataset_context in datasets_context:
        tmp_loc = dataset_context.tmp_location
        logging.info(msg=f"Dropping iceberg staging table {tmp_loc.validate_location} ...")
        catalog.drop_table(table_name=tmp_loc.validate_location, purge=False)
        logging.info(msg="Dropped successfully !")


@task(map_index_template="{{ task_name }}")
def copy_staging_to_prod(
    dataset_context: DatasetContext,
    catalog_uri: str = DEFAULT_POLARIS_HOST,
    catalog_name: str = DEFAULT_POLARIS_CATALOG,
) -> None:
    """Copy Iceberg tables from staging key to prod key"""
    context = get_current_context()
    context["task_name"] = dataset_context.dataset.name  # type: ignore

    # Get catalog
    properties = generate_catalog_properties(
        uri=catalog_uri,
    )
    catalog = IcebergCatalog(name=catalog_name, properties=properties)

    # Read staging table
    tmp_loc = dataset_context.tmp_location
    df = catalog.read_table_as_df(table_name=tmp_loc.validate_location)

    # Write prod table
    dest_loc = dataset_context.tmp_location
    catalog.write_table_and_namespace(
        df=df,
        table_status=IcebergTableStatus.PROD,
        namespace=parse_iceberg_namespace(dest_loc.validate_location),
        table_name=parse_iceberg_table(dest_loc.validate_location),
    )
