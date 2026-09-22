from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum, auto
from typing import Any

from airflow.sdk.definitions._internal.abstractoperator import TaskStateChangeCallback


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
class TaskConfig:
    task_id: str
    retries: int = 0
    retry_delay: timedelta | float = 0
    retry_exponential_backoff: bool = False
    max_retry_delay: timedelta | float | None = None
    on_execute_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_failure_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_success_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_retry_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_skipped_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None


@dataclass
class ETLStep:
    fn: Callable[..., Any]
    kwargs: dict[str, Any] | None = None
    use_context: bool = False
    read_data: bool = False
    use_previous_output: bool = False
