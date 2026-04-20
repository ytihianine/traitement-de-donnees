import logging
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Mapping

import pendulum
from src._types.dags import (
    DBParams,
    DagParams,
    DagStatus,
    FeatureFlagsEnable,
)
from src._enums.dags import FeatureFlags
from src.constants import (
    FF_CONVERT_DISABLED_MSG,
    FF_DB_DISABLED_MSG,
    FF_DOWNLOAD_GRIST_DOC_DISABLED_MSG,
    FF_MAIL_DISABLED_MSG,
    FF_S3_DISABLED_MSG,
)
import pytz

_FF_DISABLED_MESSAGES: dict[FeatureFlags, str] = {
    FeatureFlags.DB: FF_DB_DISABLED_MSG,
    FeatureFlags.S3: FF_S3_DISABLED_MSG,
    FeatureFlags.MAIL: FF_MAIL_DISABLED_MSG,
    FeatureFlags.CONVERT_FILES: FF_CONVERT_DISABLED_MSG,
    FeatureFlags.DOWNLOAD_GRIST_DOC: FF_DOWNLOAD_GRIST_DOC_DISABLED_MSG,
}

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
        try:
            tz = pytz.timezone(zone=tz_zone)
            execution_date = execution_date.astimezone(tz=tz)
        except pytz.UnknownTimeZoneError:
            raise ValueError(
                f"Invalid timezone: {tz_zone}. Must be a valid IANA timezone."
            )

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


def get_feature_flags(context: Mapping[str, Any]) -> FeatureFlagsEnable:
    """Extract and validate feature flags from context."""
    feature_flags = context.get("params", {}).get("enable", {})
    return FeatureFlagsEnable(
        db=feature_flags.get("db", False),
        mail=feature_flags.get("mail", False),
        s3=feature_flags.get("s3", False),
        convert_files=feature_flags.get("convert_files", False),
        download_grist_doc=feature_flags.get("download_grist_doc", False),
    )


def should_skip_task(
    context: Mapping[str, Any],
    feature_flag: FeatureFlags | None = None,
) -> bool:
    """Check if a task should be skipped based on DAG status and feature flags.

    Args:
        context: Airflow context
        feature_flag: Feature flag to check (e.g., FeatureFlags.DB, FeatureFlags.S3).

    Returns:
        True if the task should be skipped, False otherwise.
    """
    dag_status = get_dag_status(context=context)
    if dag_status == DagStatus.DEV:
        logging.info(msg="Dag status parameter is set to DEV -> skipping this task ...")
        return True

    if feature_flag is not None:
        flags = get_feature_flags(context=context)
        flag_value = getattr(flags, feature_flag.value)
        if not flag_value:
            msg = _FF_DISABLED_MESSAGES.get(feature_flag, f"{feature_flag} is disabled")
            logging.info(msg=msg)
            return True

    return False


def create_default_args(
    retries: int = 0, retry_delay: timedelta | None = None, **kwargs
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
    feature_flags: FeatureFlagsEnable,
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
