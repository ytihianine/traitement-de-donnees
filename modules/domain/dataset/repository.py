from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any

from modules.domain.dataset.model import DatasetContext


# =================
# Dataclasses
# =================
class DatasetContextRepository(ABC):
    @abstractmethod
    def get_list(self, nom_projet: str) -> list[DatasetContext]: ...

    @abstractmethod
    def get(self, nom_projet: str, nom_dataset: str) -> DatasetContext: ...

    @abstractmethod
    def get_list_source_fichier(self, nom_projet: str) -> list[str]: ...

    @abstractmethod
    def get_list_column_mapping(self, nom_projet: str, dataset_name: str) -> Sequence[Mapping[str, Any]]: ...
