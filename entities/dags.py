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


@dataclass
class SelecteurConfig:
    """Selecteur configuration structure matching database fields.

    Fields:
        nom_projet: Project name
        selecteur: Configuration selector
        nom_source: Source name/identifier
        filename: Name of the file
        s3_key: S3 storage key
        filepath_source_s3: Source file path in S3
        filepath_local: Local file system path
        filepath_s3: Main S3 file path
        filepath_tmp_s3: Temporary S3 file path
        tbl_name: Database table name
        tbl_order: Table processing order
    """

    nom_projet: str
    selecteur: str
    filename: str
    s3_key: str
    filepath_source_s3: str
    filepath_local: str
    filepath_s3: str
    filepath_tmp_s3: str
    nom_source: str | None = None
    tbl_name: str | None = None
    tbl_order: int | None = None


@dataclass
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


class DbInfo(TypedDict):
    projet: str
    selecteur: str
    tbl_name: str
    tbl_order: int
    is_partitionned: bool
    partition_period: str
    load_strategy: str


class SourceGrist(TypedDict):
    projet: str
    selecteur: str
    type_source: str
    id_source: str


class SourceFichier(TypedDict):
    projet: str
    selecteur: str
    type_source: str
    id_source: str
    bucket: str
    s3_key: str
    filepath_source_s3: str


class ProjetS3(TypedDict):
    projet: str
    bucket: str
    key: str
    key_tmp: str


@dataclass(frozen=True)
class SelecteurInfo:
    projet: str
    selecteur: str
    filename: str
    s3_key: str
    bucket: str
    projet_s3_key: str
    projet_s3_key_tmp: str
    filepath_s3: str
    filepath_tmp_s3: str
    tbl_name: str
    tbl_order: int
    is_partitionned: bool
    partition_period: str
    load_strategy: str


class SelecteurS3(TypedDict):
    projet: str
    selecteur: str
    filename: str
    s3_key: str
    bucket: str
    projet_s3_key: str
    projet_s3_key_tmp: str
    filepath_s3: str
    filepath_tmp_s3: str


class ColumnMapping(TypedDict):
    projet: str
    selecteur: str
    colname_source: str
    colname_dest: str


class Documentation(TypedDict):
    projet: str
    type_documentation: str
    lien: str


class Contact(TypedDict):
    projet: str
    contact_mail: str
    is_generic: bool


@dataclass(frozen=True)
class DBParams:
    prod_schema: str
    tmp_schema: str = DEFAULT_TMP_SCHEMA


class MailParams(TypedDict):
    enable: bool
    to: List[str]
    cc: List[str]
    bcc: List[str] | None


class DocsParams(TypedDict):
    lien_pipeline: str
    lien_donnees: str


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
