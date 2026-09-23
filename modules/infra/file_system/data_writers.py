from dataclasses import dataclass

import pandas as pd

from modules.domain.dataset.model import Dataset, DatasetWriter
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@dataclass(frozen=True)
class GristWriterStrategy(DatasetWriter):

    def write(self, df: pd.DataFrame, dataset: Dataset) -> None:
        raise NotImplementedError("GristWriterStrategy is not wired yet")


@dataclass(frozen=True)
class FileWriterStrategy(DatasetWriter):

    def write(self, df: pd.DataFrame, dataset: Dataset) -> None:
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(
                bucket=dataset.storage.bucket,
                connection_id=dataset.storage.s3_conn_id,
            ),
        )
        s3_handler.write(
            file_path=str(dataset.storage.get_full_s3_key(with_tmp_segment=True)),
            content=df.to_parquet(path=None, index=False),
        )


@dataclass(frozen=True)
class DbWriterStrategy(DatasetWriter):

    def write(self, df: pd.DataFrame, dataset: Dataset) -> None:
        raise NotImplementedError("DbWriterStrategy is not wired yet")
