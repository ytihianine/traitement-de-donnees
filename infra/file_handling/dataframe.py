"""DataFrame utilities for file handling."""

import io
from typing import Optional, Union
from pathlib import Path
import pandas as pd

from .base import BaseFileHandler


def read_dataframe(
    file_handler: BaseFileHandler,
    file_path: Union[str, Path],
    file_format: str = "auto",
    read_options: Optional[dict] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Read a file into a pandas DataFrame using the provided file handler.

    Args:
        file_handler: Instance of BaseFileHandler
        file_path: Path to the file to read
        file_format: Format of the file ('csv', 'excel', 'parquet', 'json', or 'auto')
        **kwargs: Additional arguments passed to the pandas read function

    Returns:
        pd.DataFrame: The loaded DataFrame

    Example:
        ```python
        # Create a file handler for S3
        handler = create_file_handler(
            's3',
            connection_id='minio_bucket',
            bucket='your-bucket'
        )

        # Read a CSV file
        df = read_dataframe(handler, 'path/to/file.csv', file_format='csv', sep=';')

        # Read a parquet file
        df = read_dataframe(handler, 'path/to/file.parquet')
        ```
    """
    # If format is auto, try to detect from file extension
    if file_format == "auto":
        ext = Path(file_path).suffix.lower()
        format_map = {
            ".csv": "csv",
            ".xlsx": "excel",
            ".xls": "excel",
            ".parquet": "parquet",
            ".json": "json",
        }
        file_format = format_map.get(ext, "csv")  # Default to CSV if unknown
    if read_options is None:
        read_options = {}
    # Read the file content

    print(f"Read data from {file_path}")
    print(f"File format: {file_format}")
    print(f"read_options: \n{read_options}")
    with file_handler.read(file_path) as file_obj:
        # Different handling based on format
        if file_format == "parquet":
            # For parquet, we need to write to a temporary BytesIO first
            buffer = io.BytesIO(file_obj.read())
            return pd.read_parquet(buffer, **read_options, **kwargs)

        elif file_format == "excel":
            buffer = io.BytesIO(file_obj.read())
            return pd.read_excel(buffer, **read_options, **kwargs)

        elif file_format == "json":
            return pd.read_json(
                io.StringIO(file_obj.read().decode("utf-8")), **read_options, **kwargs
            )

        else:  # csv
            return pd.read_csv(
                io.StringIO(file_obj.read().decode("utf-8")), **read_options, **kwargs
            )
