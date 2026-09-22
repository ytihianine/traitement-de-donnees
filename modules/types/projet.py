from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from modules.enums.dags import TypeSource


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


@dataclass(frozen=True)
class ProjetS3:
    projet: str
    bucket: str
    key: str
    key_tmp: str


@dataclass(frozen=True)
class ColumnMapping:
    projet: str
    selecteur: str
    colname_source: str
    colname_dest: str


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


# ==================
# Selecteur
# ==================
@dataclass(frozen=True)
class SelecteurConfig:
    projet: str
    selecteur: str
    # s3 info
    s3_conn_id: str
    bucket: str
    s3_key: str
    filename: str
    local_dir: str
    # db info
    tbl_name: str | None
    # Source info
    type_source: TypeSource
    id_source: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.type_source, TypeSource):
            object.__setattr__(self, "type_source", TypeSource(value=self.type_source))

    def get_full_s3_key(
        self,
        with_bucket: bool = False,
        with_tmp_segment: bool = False,
        use_id_source: bool = False,
    ) -> str:
        segments = [self.s3_key]
        if with_bucket:
            segments.insert(0, self.bucket)
        if with_tmp_segment:
            segments.append("tmp")

        if use_id_source and self.id_source is not None:
            segments.append(self.id_source)
        else:
            segments.append(self.filename)

        return "/".join(segments)

    def get_local_path(self) -> str:
        if self.filename is None:
            return str(Path(self.local_dir) / "filename_undefined")
        return str(Path(self.local_dir) / self.filename)

    def get_iceberg_namespace(self, with_bucket: bool = False) -> str:
        s3_key = self.get_full_s3_key(with_bucket=with_bucket)
        namespace_split = s3_key.split(sep=".")[0].split(sep="/")[:-1]
        return ".".join(namespace_split)

    @classmethod
    def load(
        cls,
        config: Mapping[str, Any],
    ) -> "SelecteurConfig":
        return cls(
            **config,
        )

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> "SelecteurConfig":
        return cls(
            **config,
        )
