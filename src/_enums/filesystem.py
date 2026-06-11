from enum import Enum, auto


class FileHandlerType(Enum):
    """File handler types enumeration."""

    S3 = auto()
    LOCAL = auto()


class FileFormat(Enum):
    """Supported file formats for ETL operations."""

    CSV = auto()
    EXCEL = auto()
    XLSX = auto()
    XLS = auto()
    XLSB = auto()
    PARQUET = auto()
    JSON = auto()
    AUTO = auto()


class IcebergTableStatus(Enum):

    STAGING = auto()
    PROD = auto()
