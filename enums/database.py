from enum import Enum, auto


class DatabaseType(Enum):
    """Database types enumeration."""

    POSTGRES = auto()
    SQLITE = auto()


class PartitionTimePeriod(str, Enum):
    DAY = auto()
    WEEK = auto()
    MONTH = auto()
    YEAR = auto()


class LoadStrategy(Enum):
    """Load strategies for data ingestion."""

    FULL_LOAD = auto()
    INCREMENTAL = auto()
    APPEND = auto()
