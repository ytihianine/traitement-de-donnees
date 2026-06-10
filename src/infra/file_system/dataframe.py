"""DataFrame utilities for file handling."""

import logging
from pathlib import Path
import pandas as pd

from .base import FSInterface
from .data_serializer import (
    DataSerializer,
    CSVSerializer,
    ParquetSerializer,
    ExcelSerializer,
    JSONSerializer,
)
from src._enums.filesystem import FileFormat

_serializer_registry: dict[FileFormat, DataSerializer] = {
    FileFormat.CSV: CSVSerializer(),
    FileFormat.PARQUET: ParquetSerializer(),
    FileFormat.EXCEL: ExcelSerializer(),
    FileFormat.JSON: JSONSerializer(),
}


def detect_file_extension(filepath: str | Path) -> FileFormat:
    ext = Path(filepath).suffix.lower()
    return FileFormat(value=ext[1:])


def read_dataframe(
    file_handler: FSInterface,
    file_path: str | Path,
    read_options: dict | None = None,
) -> pd.DataFrame:
    """
    Read a file into a pandas DataFrame using the provided file handler.

    Args:
        file_handler: Instance of FSInterface
        file_path: Path to the file to read
        read_options: Additional arguments passed to the serializer load function

    Returns:
        pd.DataFrame: The loaded DataFrame
    """
    file_extension = detect_file_extension(filepath=file_path)

    if read_options is None:
        read_options = {}

    logging.info(msg=f"Read data from {file_path}")
    logging.info(msg=f"File format: {file_extension}")
    logging.info(msg=f"read_options: \n{read_options}")

    # Fetch bytes from S3
    data_bytes = file_handler.read(file_path=file_path)

    # Convert bytes to DataFrame
    serializer = _serializer_registry[file_extension]
    df = serializer.load(buffer=data_bytes, **read_options)  # type: ignore
    return df
