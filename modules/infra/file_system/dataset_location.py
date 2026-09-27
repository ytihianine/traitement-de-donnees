from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from modules.domain.dataset.ports import DatasetLocationProvider
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@dataclass(frozen=True)
class S3DatasetLocationProvider(DatasetLocationProvider):
    conn_id: str
    read_options: dict = field(default_factory=dict)

    def fs_config(self, location: str, conn_id: str) -> FSConfig:
        return FSConfig(
            bucket=self.parse_s3_bucket(location=location),
            connection_id=conn_id,
        )

    def parse_s3_bucket(self, location: str) -> str:
        return location.split("/")[0]

    def parse_s3_prefix(self, location: str) -> str:
        return "/".join(location.split("/")[1:-1])

    def parse_s3_key(self, location: str) -> str:
        return "/".join(location.split("/")[1:])

    def read(
        self,
        location: str,
    ) -> pd.DataFrame:
        fs_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=self.fs_config(location=location, conn_id=self.conn_id),
        )
        df = read_dataframe(
            file_handler=fs_handler,
            file_path=self.parse_s3_key(location),
            read_options=self.read_options,
        )
        return df

    def write(
        self,
        df: pd.DataFrame,
        location: str,
    ) -> None:

        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=self.fs_config(location=location, conn_id=self.conn_id),
        )
        s3_handler.write(
            file_path=self.parse_s3_key(location=location),
            content=df.to_parquet(path=None, index=False),
        )


@dataclass(frozen=True)
class LocalFileDatasetLocationProvider(DatasetLocationProvider):
    read_options: dict = field(default_factory=dict)

    def parse_local_path(self, location: str) -> str:
        return location

    def parse_local_dir(self, location: str) -> str:
        return "/".join(location.split("/")[:-1])

    def fs_config(self, location: str) -> FSConfig:
        return FSConfig(
            base_path=self.parse_local_dir(location=location),
        )

    def read(
        self,
        location: str,
    ) -> pd.DataFrame:
        fs_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=self.fs_config(location=location),
        )
        df = read_dataframe(
            file_handler=fs_handler,
            file_path=self.parse_local_path(location=location),
            read_options=self.read_options,
        )
        return df

    def write(
        self,
        df: pd.DataFrame,
        location: str,
    ) -> None:
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=self.fs_config(location=location),
        )
        local_handler.write(
            file_path=self.parse_local_path(location=location),
            content=df.to_parquet(path=None, index=False),
        )


@dataclass(frozen=True)
class DbDatasetLocationProvider(DatasetLocationProvider):
    conn_id: str
    read_options: dict = field(default_factory=dict)

    def parse_schema(self, location: str) -> str:
        return location.split(".")[0]

    def parse_table(self, location: str) -> str:
        return location.split(".")[1]

    def read(
        self,
        location: str,
    ) -> pd.DataFrame:
        db_config = DbConfig(connection_id=self.conn_id)
        db_handler = create_db_handler(
            db_type=DatabaseType.POSTGRES,
            db_config=db_config,
        )
        schema = self.parse_schema(location=location)
        table = self.parse_table(location=location)
        df = db_handler.fetch_df(
            query=f"SELECT * FROM {schema}.{table}",
        )
        return df

    def write(
        self,
        df: pd.DataFrame,
        location: str,
    ) -> None:
        raise NotImplementedError("DbDatasetWriter is not wired yet")


@dataclass(frozen=True)
class GristDatasetLocationProvider(DatasetLocationProvider):
    def parse_doc_id(self, location: str) -> str:
        return location.split(".")[0]

    def parse_table_id(self, location: str) -> str:
        return location.split(".")[1]

    def read(
        self,
        location: str,
    ) -> pd.DataFrame:
        doc_id = self.parse_doc_id(location=location)
        table_id = self.parse_table_id(location=location)
        doc_local_path = Path("/tmp") / f"{doc_id}.sqlite"

        sqlite_handler = create_db_handler(
            db_type=DatabaseType.SQLITE,
            db_config=DbConfig(
                db_path=str(doc_local_path),
            ),
        )

        # Read the requested table
        df = sqlite_handler.fetch_df(query=f"SELECT * FROM {table_id}")

        return df

    def write(
        self,
        df: pd.DataFrame,
        location: str,
    ) -> None:
        raise NotImplementedError("GristDatasetWriter is not wired yet")
