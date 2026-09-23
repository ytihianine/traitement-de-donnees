from abc import ABC, abstractmethod
from dataclasses import dataclass

from modules.domain.dataset.model import Dataset, DatasetStorage


# =================
# Dataclasses
# =================
@dataclass(frozen=True)
class DatasetRepository(ABC):
    @abstractmethod
    def get(self, id_projet: int, name: str) -> Dataset: ...

    @abstractmethod
    def list(self, id_projet: int) -> list[Dataset]: ...


@dataclass(frozen=True)
class DatasetStorageRepository(ABC):

    @abstractmethod
    def get(self, dataset: Dataset) -> DatasetStorage: ...

    @abstractmethod
    def list(self, id_projet: int) -> list[DatasetStorage]: ...
