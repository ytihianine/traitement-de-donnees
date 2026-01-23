from typing import Any


from enum import Enum, auto


class DatabaseType(Enum):
    """Database types enumeration."""

    POSTGRES = auto()
    SQLITE = auto()


class PartitionTimePeriod(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name.upper()

    DAY = auto()
    WEEK = auto()
    MONTH = auto()
    YEAR = auto()


class LoadStrategy(Enum):
    """Load strategies for data ingestion."""

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name.upper()

    FULL_LOAD = auto()
    INCREMENTAL = auto()
    APPEND = auto()
