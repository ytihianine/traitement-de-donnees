from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Mapping, Optional

import pendulum
from types.dags import (
    DBParams,
    DagParams,
    DagStatus,
    FeatureFlags,
)
import pytz

DEFAULT_OWNER = "airflow"
DEFAULT_EMAIL_TO = ["yanis.tihianine@finances.gouv.fr"]
DEFAULT_EMAIL_CC = ["labo-data@finances.gouv.fr"]
DEFAULT_TMP_SCHEMA = "temporaire"


def get_project_name(context: Mapping[str, Any]) -> str:
    """Extract and validate project name from context."""
    nom_projet = context.get("params", {}).get("nom_projet")
    if not nom_projet:
        raise ValueError("nom_projet must be defined in DAG parameters")
    return nom_projet


def get_dag_status(context: Mapping[str, Any]) -> DagStatus:
    """Extract and validate project name from context."""
    dag_status = context.get("params", {}).get("dag_status")
    if not dag_status:
        raise ValueError("dag_status must be defined in DAG parameters")
    return DagStatus(value=dag_status)


def get_execution_date(
    context: Mapping[str, Any], use_tz: bool = False, tz_zone: str = "Europe/Paris"
) -> datetime:
    """Extract and validate execution date from context."""
    execution_date = context.get("data_interval_start")

    if not execution_date or not isinstance(execution_date, datetime):
        raise ValueError("Invalid execution date in Airflow context")

    if use_tz:
        execution_date = execution_date.astimezone(tz=pytz.timezone(zone=tz_zone))

    return execution_date


def get_db_info(context: Mapping[str, Any]) -> DBParams:
    """Extract and validate database info from context."""
    db_params = context.get("params", {}).get("db", {})
    prod_schema = db_params.get("prod_schema")
    tmp_schema = db_params.get("tmp_schema")

    if not prod_schema:
        raise ValueError("prod_schema must be defined in DAG parameters under db")
    if not tmp_schema:
        raise ValueError("tmp_schema must be defined in DAG parameters under db")

    return DBParams(prod_schema=prod_schema, tmp_schema=tmp_schema)


def create_default_args(
    retries: int = 0, retry_delay: Optional[timedelta] = None, **kwargs
) -> dict:
    """Create standard default_args for DAGs."""
    args = {
        "owner": DEFAULT_OWNER,
        "depends_on_past": False,
        "start_date": pendulum.today(tz="UTC").add(days=-1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
    }
    if retry_delay:
        args["retry_delay"] = retry_delay
    args.update(kwargs)
    return args


def create_dag_params(
    nom_projet: str,
    dag_status: DagStatus,
    db_params: DBParams | None,
    feature_flags: FeatureFlags,
) -> dict:
    """Create standard params for DAGs."""
    # Using DagParams for type checking
    dag_params = DagParams(
        nom_projet=nom_projet,
        dag_status=dag_status.value,
        db=db_params,
        enable=feature_flags,
    )

    return asdict(obj=dag_params)
