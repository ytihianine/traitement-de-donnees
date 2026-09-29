from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from uuid import UUID


# =================
# Enums
# =================
class TypeDocumentation(StrEnum):
    """Type de documentation"""

    PIPELINE = "pipeline"
    DATA = "data"

    def serialize(self) -> str:
        return self.value

    def deserialize(self) -> "TypeDocumentation":
        return TypeDocumentation(self.value)


# =================
# Dataclasses
# =================
@dataclass(frozen=True)
class Documentation:
    id_projet: int
    type_documentation: TypeDocumentation
    lien: str


@dataclass(frozen=True)
class Contact:
    id_projet: int
    contact_mail: str
    is_mail_generic: bool


@dataclass(frozen=True)
class ProjetLocation:
    id_projet: int
    projet: str
    bucket: str
    fs_folder: str
    fs_folder_tmp: str
    db_schema: str


@dataclass(frozen=True)
class ProjetMetadata:
    id_projet: int
    snapshot_id: UUID
    snapshot_id_parent: UUID
    import_timestamp: datetime
    is_dag_completed: bool


@dataclass
class Projet:
    name: str
    id: int | None = None
