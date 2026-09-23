from dataclasses import dataclass
from enum import Enum


# =================
# Enums
# =================
class TypeSource(Enum):
    """Type de source de données"""

    GRIST = "Grist"
    FILE = "Fichier"


# =================
# Dataclasses
# =================
@dataclass(frozen=True)
class Dataset:
    id_projet: int
    name: str


@dataclass(frozen=True)
class DatasetStorage:
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
