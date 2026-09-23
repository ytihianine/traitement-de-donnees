from abc import ABC, abstractmethod

import pandas as pd

from modules.domain.dataset.model import Dataset, DatasetStorage


# =================
# Dataclasses
# =================
class DatasetRepository(ABC):
    @abstractmethod
    def get(self, id_projet: int, name: str) -> Dataset: ...

    @abstractmethod
    def get_list(self, id_projet: int) -> list[Dataset]: ...


class DatasetStorageRepository(ABC):

    @abstractmethod
    def get(self, dataset: Dataset) -> DatasetStorage: ...

    @abstractmethod
    def get_list(self, id_projet: int) -> list[DatasetStorage]: ...

    @abstractmethod
    def get_list_source_fichier(self, id_projet: int) -> list[str]: ...

    @abstractmethod
    def get_list_column_mapping_as_df(self, id_projet: int, selecteur: str) -> pd.DataFrame: ...
