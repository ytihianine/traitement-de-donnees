"""Utilities to validate DAG `params` at runtime."""

import logging
from collections.abc import Mapping
from typing import Any

from airflow.sdk import task

from modules.domain.dag.model import DagConfig


@task(task_id="validate_dag_params")
def validate_dag_parameters(**context: Mapping[str, Any]) -> None:
    """Validate that params conform to DagConfig structure.

    Returns a list of error messages. Empty list means validation passed.
    """
    params = context.get("params")

    if params is None:
        raise AttributeError("DAG params are required")

    if not isinstance(params, dict):
        raise AttributeError("DAG params must be a dictionary")

    # Init class to check for errors.
    DagConfig.from_dag_context(context_params=params)

    logging.info("DAG params validation passed")
