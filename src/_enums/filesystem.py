from enum import Enum, auto


class FileHandlerType(Enum):
    """File handler types enumeration."""

    S3 = auto()
    LOCAL = auto()


class FileFormat(Enum):
    """Supported file formats for ETL operations."""

    CSV = "csv"
    EXCEL = "excel"
    XLSX = "excel"
    XLS = "excel"
    XLSB = "excel"
    PARQUET = "parquet"
    JSON = "json"
    AUTO = "auto"


class IcebergTableStatus(Enum):

    STAGING = auto()
    PROD = auto()
