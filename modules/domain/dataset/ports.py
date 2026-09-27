from abc import ABC, abstractmethod

import pandas as pd

from modules.domain.dataset.model import DatasetLocation


class DatasetStore(ABC):

    @abstractmethod
    def read(
        self,
        dataset_location: DatasetLocation,
        use_destination: bool = False,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def write(self, df: pd.DataFrame, dataset_location: DatasetLocation) -> None: ...
