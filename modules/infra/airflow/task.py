from dataclasses import dataclass
from datetime import timedelta

from airflow.sdk.definitions._internal.abstractoperator import TaskStateChangeCallback


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
