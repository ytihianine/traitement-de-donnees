from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID


def custom_asdict_factory(data) -> dict[str, Any]:
    """
    Custom factory function for dataclasses asdict function
    to convert Enum values to their actual values instead of Enum instances.
    """
    from enum import Enum

    def convert_value(obj) -> Any:
        if isinstance(obj, Enum):
            return obj.value
        return obj

    return dict((k, convert_value(obj=v)) for k, v in data)


# =================
# Enums
# =================
class TypeDocumentation(Enum):
    """Type de documentation"""

    PIPELINE = "pipeline"
    DATA = "data"


# =================
# Dataclasses
# =================
@dataclass(frozen=True)
class Documentation:
    id_projet: int
    type_documentation: str
    lien: str


@dataclass(frozen=True)
class Contact:
    id_projet: int
    contact_mail: str
    is_mail_generic: bool


@dataclass(frozen=True)
class ProjetS3:
    projet: str
    bucket: str
    key: str
    key_tmp: str


@dataclass(frozen=True)
class ProjetMetadata:
    id_projet: int
    snapshot_id: UUID
    snapshot_id_parent: UUID
    import_timestamp: datetime
    status: bool


@dataclass
class Projet:
    name: str
    id: int
