from abc import ABC, abstractmethod

import pandas as pd


class DatasetLocationProvider(ABC):
    @abstractmethod
    def read(
        self,
        location: str,
    ) -> pd.DataFrame: ...

    @abstractmethod
    def write(self, content: bytes, location: str) -> None: ...
