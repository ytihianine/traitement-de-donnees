from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from modules.domain.dataset.model import DatasetLocation


class DatasetLocationProvider(ABC):
    @abstractmethod
    def read(
        self,
        location: str,
        read_options: dict[str, Any],
    ) -> pd.DataFrame: ...

    @abstractmethod
    def write(self, content: bytes, location: str) -> None: ...


class DatasetLocationProviderFactory(ABC):
    @abstractmethod
    def create(self, dataset_location: DatasetLocation) -> DatasetLocationProvider: ...
