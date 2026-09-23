from abc import ABC, abstractmethod

import pandas as pd

from modules.domain.dataset.model import Dataset


class DatasetReader(ABC):

    @abstractmethod
    def read(
        self,
        dataset: Dataset,
    ) -> pd.DataFrame: ...


class DatasetWriter(ABC):

    @abstractmethod
    def write(self, df: pd.DataFrame, dataset: Dataset) -> None: ...
