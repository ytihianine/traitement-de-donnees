from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from modules.domain.dataset.model import Dataset


class LoadStrategy(Enum):
    """Load strategies for data ingestion."""

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name.upper()

    FULL_LOAD = "FULL_LOAD"
    INCREMENTAL = "INCREMENTAL"
    APPEND = "APPEND"

    def serialize(self) -> str:
        return self.value

    def deserialize(self) -> "LoadStrategy":
        return LoadStrategy(self.value)


class PartitionTimePeriod(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name.upper()

    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"

    def serialize(self) -> str:
        return self.value

    def deserialize(self) -> "PartitionTimePeriod":
        return PartitionTimePeriod(self.value)


def determine_partition_period(time_period: PartitionTimePeriod, execution_date: datetime) -> tuple[datetime, datetime]:
    """Determine the start and end dates for a partition based on the time period."""
    if time_period == PartitionTimePeriod.YEAR:
        from_date_period = execution_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        to_date_period = from_date_period.replace(year=from_date_period.year + 1)
    elif time_period == PartitionTimePeriod.MONTH:
        from_date_period = execution_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if from_date_period.month == 12:
            to_date_period = from_date_period.replace(year=from_date_period.year + 1, month=1)
        else:
            to_date_period = from_date_period.replace(month=from_date_period.month + 1)
    elif time_period == PartitionTimePeriod.WEEK:
        from_date_period = execution_date - timedelta(days=execution_date.weekday())
        from_date_period = from_date_period.replace(hour=0, minute=0, second=0, microsecond=0)
        to_date_period = from_date_period + timedelta(weeks=1)
    elif time_period == PartitionTimePeriod.DAY:
        from_date_period = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
        to_date_period = from_date_period + timedelta(days=1)
    else:
        raise ValueError(f"Unsupported time period: {time_period}")
    return (from_date_period, to_date_period)


@dataclass(frozen=True)
class ExecutionOptions:
    read_options: dict[str, Any] = field(default_factory=dict)
    # Database
    tbl_order: int = 0
    keep_file_id_col: bool = True
    is_partitioned: bool = True
    partition_period: PartitionTimePeriod = PartitionTimePeriod.DAY
    load_strategy: LoadStrategy = LoadStrategy.APPEND


@dataclass(frozen=True)
class PipelineDescriptor:
    input_datasets: tuple[Dataset, ...] | None
    output_dataset: Dataset
    operation: Callable[..., object | None]
    use_input_results_as_operation_args: bool = False
    add_metadata: bool = True
    datasets_context_task_id: str = "get_projet_datasets_context"
