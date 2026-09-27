from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from modules.domain.projet.model import Projet


# =================
# Enums
# =================
class TypeLocation(Enum):
    """Type de source de données"""

    GRIST = "Grist"
    S3_FILE = "S3"
    LOCAL_FILE = "Local"
    ICEBERG = "Iceberg"
    DB = "Database"


# =================
# Dataclasses
# =================
@dataclass(frozen=True)
class Dataset:
    name: str


@dataclass(frozen=True)
class DatasetLocation:
    type_source: TypeLocation
    source_location: str | None = None
    dest_location: str | None = None
    conn_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.type_source, TypeLocation) and self.type_source is not None:
            object.__setattr__(self, "type_source", TypeLocation(value=self.type_source))

    # Database properties
    @property
    def db_schema(self) -> str:
        if self.source_location is not None:
            return self.source_location.split(sep=".")[0]
        raise ValueError("source_location is None. Can't extract db schema")

    @property
    def db_table(self) -> str:
        if self.source_location is not None:
            return self.source_location.split(sep=".")[1]
        raise ValueError("source_location is None. Can't extract db table")

    # S3 properties
    @property
    def s3_bucket(self) -> str:
        if self.source_location is not None:
            return self.source_location.split(sep="/")[0]
        raise ValueError("source_location is None. Can't extract s3 bucket")

    @property
    def s3_prefix(self) -> str:
        if self.source_location is not None:
            return "/".join(self.source_location.split(sep="/")[:-1])
        raise ValueError("source_location is None. Can't extract s3 prefix")

    @property
    def s3_key(self) -> str:
        if self.source_location is not None:
            return "/".join(self.source_location.split(sep="/")[1:])
        raise ValueError("source_location is None. Can't extract s3 key")

    # Local file properties
    @property
    def local_dir(self) -> str:
        if self.source_location is not None:
            return str(Path(self.source_location).parent)
        raise ValueError("source_location is None. Can't extract local directory")

    @property
    def local_file(self) -> str:
        if self.source_location is not None:
            return str(Path(self.source_location).name)
        raise ValueError("source_location is None. Can't extract local file")

    # Grist properties
    @property
    def grist_doc_id(self) -> str:
        if self.source_location is not None:
            return self.source_location.split(sep=".")[0]
        raise ValueError("source_location is None. Can't extract grist doc id")

    @property
    def grist_table_id(self) -> str:
        if self.source_location is not None:
            return self.source_location.split(sep=".")[1]
        raise ValueError("source_location is None. Can't extract grist table id")

    # Iceberg
    @property
    def iceberg_namespace(self) -> str:
        if self.source_location is not None:
            return self.source_location.split(sep=".")[0]
        raise ValueError("source_location is None. Can't extract iceberg namespace")

    @property
    def iceberg_table(self) -> str:
        if self.source_location is not None:
            return self.source_location.split(sep=".")[1]
        raise ValueError("source_location is None. Can't extract iceberg table")


@dataclass(frozen=True)
class DatasetContext:
    projet: Projet
    dataset: Dataset
    dataset_location: DatasetLocation

    @property
    def projet_name(self) -> str:
        return self.projet.name

    @property
    def projet_id(self) -> int:
        return self.projet.id

    @property
    def dataset_name(self) -> str:
        return self.dataset.name
