from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field

import pandas as pd

from modules.domain.selecteur.model import SelecteurConfig
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@dataclass
class DataContext:
    """Runtime container shared across the pipeline."""

    datasets: dict[str, pd.DataFrame] = field(default_factory=dict)

    def add(self, name: str, df: pd.DataFrame) -> None:
        self.datasets[name] = df

    def get(self, name: str) -> pd.DataFrame:
        return self.datasets[name]

    def replace(self, name: str, df: pd.DataFrame) -> None:
        self.datasets[name] = df

    def exists(self, name: str) -> bool:
        return name in self.datasets


@dataclass(frozen=True)
class ReaderStrategy(ABC):
    context: DataContext = field(default_factory=DataContext)

    @abstractmethod
    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext: ...


@dataclass(frozen=True)
class FileReaderStrategy(ReaderStrategy):
    fs_type: FileHandlerType = field(default=FileHandlerType.S3)

    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext:
        fs_handler = create_file_handler(
            handler_type=self.fs_type,
            config=FSConfig(
                bucket=selecteur.bucket,
                connection_id=selecteur.execution_options.s3_conn_id,
            ),
        )
        df = read_dataframe(
            file_handler=fs_handler,
            file_path=selecteur.get_full_s3_key(use_id_source=True),
            read_options=selecteur.execution_options.read_options,
        )
        self.context.add(name=selecteur.selecteur, df=df)
        return self.context


@dataclass(frozen=True)
class DbReaderStrategy(ReaderStrategy):
    query: str = field(default="SELECT * FROM my_table")

    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext:
        db_handler = create_db_handler(
            db_type=DatabaseType.POSTGRES,
            db_config=DbConfig(
                connection_id=selecteur.execution_options.db_conn_id,
            ),
        )
        df = db_handler.fetch_df(query=self.query)
        self.context.add(name=selecteur.selecteur, df=df)
        return self.context
