from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass, field
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


@dataclass(frozen=True)
class PipelineDescriptor(ABC):
    input_datasets: tuple[Dataset]
    output_dataset: Dataset
    transformations: tuple[Callable[..., pd.DataFrame]]
    add_metadata: bool = True
    selecteur_config_task_id: str = "get_selecteur_config"
