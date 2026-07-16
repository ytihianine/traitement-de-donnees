from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

from project.enums.filesystem import FileHandlerType
from project.infra.file_system.factory import create_file_handler
from project.types.projet import SelecteurConfig


class WriterStrategy(ABC):

    @abstractmethod
    def write(self, df: pd.DataFrame, selecteur: SelecteurConfig) -> None: ...


@dataclass(frozen=True)
class GristWriterStrategy(WriterStrategy):

    def write(self, df: pd.DataFrame, selecteur: SelecteurConfig) -> None:
        raise NotImplementedError("GristWriterStrategy is not wired yet")


@dataclass(frozen=True)
class FileWriterStrategy(WriterStrategy):

    def write(self, df: pd.DataFrame, selecteur: SelecteurConfig) -> None:
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            connection_id=selecteur.storage_options.s3_conn_id,
            bucket=selecteur.storage_info.bucket,
        )
        s3_handler.write(
            file_path=str(selecteur.storage_info.get_full_s3_key(with_tmp_segment=True)),
            content=df.to_parquet(path=None, index=False),
        )


@dataclass(frozen=True)
class DbWriterStrategy(WriterStrategy):

    def write(self, df: pd.DataFrame, selecteur: SelecteurConfig) -> None:
        raise NotImplementedError("DbWriterStrategy is not wired yet")
