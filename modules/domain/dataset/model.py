from dataclasses import dataclass
from enum import Enum

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
    type_location: TypeLocation
    source_location: str | None = None
    dest_location: str | None = None
    conn_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.type_location, TypeLocation) and self.type_location is not None:
            object.__setattr__(self, "type_location", TypeLocation(value=self.type_location))

    def location(self, use_destination: bool = False) -> str | None:
        return self.dest_location if use_destination else self.source_location


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
