from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any

import pandas as pd

from modules.constants import DEFAULT_PG_DATA_CONN_ID, DEFAULT_S3_CONN_ID
from modules.domain.dataset.model import Dataset


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


@dataclass(frozen=True)
class ExecutionOptions:
    export_result: bool = True
    add_metadata: bool = True
    # S3
    s3_conn_id: str = DEFAULT_S3_CONN_ID
    write_to_s3: bool = True
    write_to_s3_with_iceberg: bool = True
    read_options: dict[str, Any] = field(default_factory=dict)
    # Database
    db_conn_id: str = DEFAULT_PG_DATA_CONN_ID
    write_to_db: bool = True
    use_prod_schema: bool = True
    tbl_order: int = 0
    keep_file_id_col: bool = True
    is_partitioned: bool = True
    partition_period: PartitionTimePeriod = PartitionTimePeriod.DAY
    load_strategy: LoadStrategy = LoadStrategy.APPEND

    def determine_partition_period(
        self, time_period: PartitionTimePeriod, execution_date: datetime
    ) -> tuple[datetime, datetime]:
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
class PipelineDescriptor:
    input_datasets: tuple[Dataset]
    output_dataset: Dataset
    transformations: tuple[Callable[..., pd.DataFrame]]
    add_metadata: bool = True
    selecteur_config_task_id: str = "get_selecteur_config"
