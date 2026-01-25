from datetime import timedelta
from dataclasses import dataclass
from typing import TypedDict, List, ParamSpec, TypeVar, Callable, Any

from airflow.sdk.definitions._internal.abstractoperator import TaskStateChangeCallback

from enums.dags import DagStatus
from utils.config.vars import DEFAULT_TMP_SCHEMA

# ==================
# Dags
# ==================
P = ParamSpec(name="P")
R = TypeVar(name="R")


@dataclass(frozen=True)
class TaskConfig:
    task_id: str
    retries: int = 0
    retry_delay: timedelta | float = 0
    retry_exponential_backoff: bool = False
    max_retry_delay: timedelta | float | None = None
    on_execute_callback: (
        None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    ) = None
    on_failure_callback: (
        None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    ) = None
    on_success_callback: (
        None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    ) = None
    on_retry_callback: (
        None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    ) = None
    on_skipped_callback: (
        None | TaskStateChangeCallback | list[TaskStateChangeCallback]
    ) = None


@dataclass
class ETLStep:
    fn: Callable[..., Any]
    kwargs: dict[str, Any] | None = None
    use_context: bool = False
    read_data: bool = False
    use_previous_output: bool = False


@dataclass(frozen=True)
class DBParams:
    prod_schema: str
    tmp_schema: str = DEFAULT_TMP_SCHEMA


@dataclass(frozen=True)
class FeatureFlags:
    db: bool
    mail: bool
    s3: bool
    convert_files: bool
    download_grist_doc: bool


@dataclass(frozen=True)
class DagParams:
    nom_projet: str
    dag_status: DagStatus | int
    db: DBParams | None
    enable: FeatureFlags


# Top level keys
KEY_NOM_PROJET = "nom_projet"
KEY_DB_STATUS = "dag_status"
KEY_DB = "db"
KEY_MAIL = "mail"
KEY_MAIL_ENABLE = f"{KEY_MAIL}.enable"
KEY_MAIL_TO = f"{KEY_MAIL}.to"
KEY_MAIL_CC = f"{KEY_MAIL}.cc"
KEY_DOCS = "docs"
KEY_DOCS_LIEN_PIPELINE = f"{KEY_DOCS}.lien_pipeline"
KEY_DOCS_LIEN_DONNEES = f"{KEY_DOCS}.lien_donnees"


# Nested keys with their paths
ALL_PARAM_PATHS = [
    KEY_NOM_PROJET,
    f"{KEY_DB}.prod_schema",
    f"{KEY_DB}.tmp_schema",
    KEY_MAIL_ENABLE,
    KEY_MAIL_TO,
    KEY_MAIL_CC,
    KEY_DOCS_LIEN_PIPELINE,
    KEY_DOCS_LIEN_DONNEES,
]

# Optional keys that don't need validation
OPTIONAL_PATHS = [
    f"{KEY_MAIL}.bcc",
]
