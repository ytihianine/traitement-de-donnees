from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from modules.constants import DEFAULT_TMP_SCHEMA


class DagStatus(Enum):
    """DAG status"""

    RUN = auto()
    DEV = auto()


@dataclass(frozen=True)
class DBParams:
    prod_schema: str
    tmp_schema: str = DEFAULT_TMP_SCHEMA

    @classmethod
    def from_dag_context(cls, context_params: dict) -> "DBParams":
        if "db" not in context_params:
            raise AttributeError("Field 'db' is required")

        db_params = context_params["db"]

        if not isinstance(db_params, dict):
            raise AttributeError("Field 'db' must be a dictionary")

        if "prod_schema" not in db_params:
            raise AttributeError("Field 'prod_schema' is required in 'db'")

        return cls(
            prod_schema=db_params["prod_schema"],
            tmp_schema=db_params.get("tmp_schema", DEFAULT_TMP_SCHEMA),
        )


@dataclass(frozen=True)
class FeatureFlagsEnable:
    db: bool
    mail: bool
    s3: bool
    convert_files: bool
    download_grist_doc: bool


def is_key_in_dict(key: str, d: dict[str, Any]) -> bool:
    """Check if a key is present in a dictionary and not None."""
    return key in d


@dataclass(frozen=True)
class DagConfig:
    nom_projet: str
    dag_status: DagStatus | int
    db: DBParams | None
    enable: FeatureFlagsEnable

    @classmethod
    def from_dag_context(cls, context_params: dict) -> "DagConfig":
        errors: list[str] = []

        # Check keys are in context
        if "nom_projet" not in context_params:
            errors.append("Field 'nom_projet' is required")

        if "dag_status" not in context_params:
            errors.append("Field 'dag_status' is required")

        if "db" not in context_params:
            errors.append("Field 'db' is required")

        if "enable" not in context_params:
            errors.append("Field 'enable' is required")

        if len(errors) > 0:
            raise AttributeError("DAG params validation failed.")

        return cls(
            nom_projet=context_params["nom_projet"],
            dag_status=DagStatus(value=context_params["dag_status"]),
            db=DBParams.from_dag_context(context_params),
            enable=FeatureFlagsEnable(**context_params["enable"]),
        )
