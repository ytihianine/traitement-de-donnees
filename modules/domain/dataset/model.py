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
    nom_projet: str
    name: str
