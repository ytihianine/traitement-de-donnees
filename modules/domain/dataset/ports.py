from abc import ABC, abstractmethod

import pandas as pd

from modules.domain.dataset.model import StorageInfo


class DatasetReader(ABC):

    @abstractmethod
    def read(
        self,
        storage_info: StorageInfo,
    ) -> pd.DataFrame: ...


class DatasetWriter(ABC):

    @abstractmethod
    def write(self, df: pd.DataFrame, storage_info: StorageInfo) -> None: ...
