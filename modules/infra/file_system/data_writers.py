from dataclasses import dataclass

import pandas as pd

from modules.domain.dataset.model import Dataset, DatasetWriter
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@dataclass(frozen=True)
class GristDatasetWriter(DatasetWriter):

    def write(self, df: pd.DataFrame, dataset: Dataset) -> None:
        raise NotImplementedError("GristDatasetWriter is not wired yet")


@dataclass(frozen=True)
class FileDatasetWriter(DatasetWriter):
    fs_config: FSConfig
    fs_type: FileHandlerType = FileHandlerType.S3

    def write(self, df: pd.DataFrame, dataset: Dataset) -> None:
        s3_handler = create_file_handler(
            handler_type=self.fs_type,
            config=self.fs_config,
        )
        s3_handler.write(
            file_path=str(dataset.storage.get_full_s3_key(with_tmp_segment=True)),
            content=df.to_parquet(path=None, index=False),
        )


@dataclass(frozen=True)
class DbDatasetWriter(DatasetWriter):

    def write(self, df: pd.DataFrame, dataset: Dataset) -> None:
        raise NotImplementedError("DbDatasetWriter is not wired yet")
