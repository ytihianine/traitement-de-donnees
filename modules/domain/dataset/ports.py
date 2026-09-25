from abc import ABC, abstractmethod

import pandas as pd

from modules.domain.dataset.model import Dataset, StorageInfo


class DatasetReader(ABC):

    @abstractmethod
    def read(
        self,
        dataset: Dataset,
    ) -> pd.DataFrame: ...


class DatasetWriter(ABC):

    @abstractmethod
    def write(self, df: pd.DataFrame, dataset: Dataset) -> None: ...


class StorageInfoProvider(ABC):
    @abstractmethod
    def get_by_dataset(self, nom_projet: str, dataset_name: str) -> StorageInfo: ...

    @abstractmethod
    def get_by_projet(self, nom_projet: str) -> list[StorageInfo]: ...

    @abstractmethod
    def get_list_source_fichier(self, nom_projet: str) -> list[str]: ...
