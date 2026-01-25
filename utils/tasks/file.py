"""File processing task utilities using infrastructure handlers."""

import logging
from typing import Any, Callable, Dict, Optional

from airflow.sdk import task
import pandas as pd

from infra.file_handling.factory import create_default_s3_handler
from infra.file_handling.dataframe import read_dataframe

from utils.config.dag_params import get_project_name
from utils.dataframe import df_info
from utils.config.tasks import (
    get_cols_mapping,
    format_cols_mapping,
    get_selecteur_s3,
    get_selecteur_source_fichier,
)

TaskParams = Dict[str, Any]


def create_parquet_converter_task(
    selecteur: str,
    task_params: Optional[TaskParams] = None,
    process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    read_options: Optional[dict[str, Any]] = None,
    apply_cols_mapping: bool = True,
) -> Callable:
    """Create a task that converts files to Parquet format.

    Args:
        task_params: Airflow task parameters
        selecteur: Selector to process
        process_func: Optional function to process DataFrame
        read_options: Optional read options for the input file

    Returns:
        Task function that performs the conversion

    Raises:
        ValueError: If task_id not provided in task_params
    """
    if task_params is None:
        task_params = {"task_id": selecteur}

    if "task_id" not in task_params:
        raise ValueError("task_params must include 'task_id'")

    @task(**task_params)
    def convert_to_parquet(**context) -> None:
        """Convert file to Parquet format and upload to S3."""
        s3_handler = create_default_s3_handler()

        nom_projet = get_project_name(context=context)

        # Get input file path
        logging.info(
            msg=f"Getting configuration for project {nom_projet} and selector {selecteur}"
        )
        source = get_selecteur_source_fichier(
            nom_projet=nom_projet, selecteur=selecteur
        )
        output = get_selecteur_s3(nom_projet=nom_projet, selecteur=selecteur)

        # Read input file based on extension
        logging.info(msg=f"Reading file from {source.filepath_source_s3}")
        df = read_dataframe(
            file_handler=s3_handler,
            file_path=source.filepath_source_s3,
            read_options=read_options,
        )

        df_info(df, df_name=f"{task_params['task_id']} - Initial state")

        if apply_cols_mapping:
            # Apply column mapping if available
            cols_mapping = get_cols_mapping(nom_projet=nom_projet, selecteur=selecteur)
            if cols_mapping.empty:
                print(f"No column mapping found for selecteur {selecteur}")
            else:
                cols_mapping = format_cols_mapping(df_cols_map=cols_mapping)
                df = df.set_axis(
                    labels=[" ".join(colname.split()) for colname in df.columns],
                    axis="columns",
                )
                df = df.rename(columns=cols_mapping, errors="raise")
                df = df.drop(columns=list(set(df.columns) - set(cols_mapping.values())))

        # Apply custom processing
        if process_func:
            df = process_func(df)
            df_info(df, df_name=f"{task_params['task_id']} - After processing")

        # Convert to parquet and save
        parquet_data = df.to_parquet(path=None, index=False)
        logging.info(msg=f"Saving to {output.filepath_tmp_s3}")
        s3_handler.write(file_path=output.filepath_tmp_s3, content=parquet_data)
        logging.info(msg=f"Successfully saved parquet file to {output.filepath_tmp_s3}")

    return convert_to_parquet
