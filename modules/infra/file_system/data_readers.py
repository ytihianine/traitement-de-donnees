from dataclasses import dataclass, field

import pandas as pd

from modules.domain.dataset.model import StorageInfo
from modules.domain.dataset.ports import DatasetReader
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@dataclass(frozen=True)
class FileDatasetReader(DatasetReader):
    fs_config: FSConfig
    fs_type: FileHandlerType = field(default=FileHandlerType.S3)
    read_options: dict = field(default_factory=dict)

    def read(
        self,
        storage_info: StorageInfo,
    ) -> pd.DataFrame:
        fs_handler = create_file_handler(
            handler_type=self.fs_type,
            config=self.fs_config,
        )
        df = read_dataframe(
            file_handler=fs_handler,
            file_path=storage_info.get_full_s3_key(use_id_source=True),
            read_options=self.read_options,
        )
        return df


@dataclass(frozen=True)
class DbDatasetReader(DatasetReader):
    query: str
    db_type: DatabaseType = field(default=DatabaseType.POSTGRES)
    db_config: DbConfig = field(default_factory=DbConfig)

    def read(
        self,
        storage_info: StorageInfo,
    ) -> pd.DataFrame:
        db_handler = create_db_handler(
            db_type=self.db_type,
            db_config=self.db_config,
        )
        df = db_handler.fetch_df(query=self.query)
        return df
