from abc import ABC, abstractmethod
from collections.abc import Mapping

from modules.domain.dataset.model import Dataset


# =================
# Dataclasses
# =================
class DatasetRepository(ABC):
    @abstractmethod
    def get(self, nom_projet: str, name: str) -> Dataset: ...

    @abstractmethod
    def get_list(self, nom_projet: str) -> list[Dataset]: ...

    @abstractmethod
    def get_list_source_fichier(self, nom_projet: str) -> list[str]: ...

    @abstractmethod
    def get_list_column_mapping(self, nom_projet: str, dataset_name: str) -> list[Mapping[str, str]]: ...
