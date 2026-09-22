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
    projet: str
    type_documentation: str
    lien: str


@dataclass(frozen=True)
class Contact:
    projet: str
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
    _id_projet: int | None = None
    _snapshot_id: UUID | None = None
    _snapshot_id_parent: UUID | None = None
    _import_timestamp: datetime | None = None

    @property
    def id_projet(self) -> int:
        if self._id_projet is None:
            raise ValueError("id_projet is not set")
        return self._id_projet

    @property
    def snapshot_id(self) -> UUID:
        if self._snapshot_id is None:
            raise ValueError("snapshot_id is not set")
        if not isinstance(self._snapshot_id, UUID):
            raise TypeError("snapshot_id must be a UUID")

        return self._snapshot_id

    @property
    def snapshot_id_parent(self) -> UUID | None:
        if self._snapshot_id_parent is not None and not isinstance(self._snapshot_id_parent, UUID):
            raise TypeError("snapshot_id_parent must be a UUID or None")
        return self._snapshot_id_parent

    @property
    def import_timestamp(self) -> datetime:
        if self._import_timestamp is None:
            raise ValueError("import_timestamp is not set")
        if not isinstance(self._import_timestamp, datetime):
            raise TypeError("import_timestamp must be a datetime")
        return self._import_timestamp
