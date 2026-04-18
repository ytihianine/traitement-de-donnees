from enum import Enum, auto


class FileHandlerType(Enum):
    """File handler types enumeration."""

    S3 = auto()
    LOCAL = auto()


class FileFormat(Enum):
    """Supported file formats for ETL operations."""

    CSV = "csv"
    EXCEL = "excel"
    PARQUET = "parquet"
    JSON = "json"


class IcebergTableStatus(Enum):

    STAGING = auto()
    PROD = auto()
