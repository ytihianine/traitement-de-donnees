from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from modules.domain.dataset.ports import DatasetLocationProvider
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


# Parsing functions for different dataset locations
def parse_s3_bucket(location: str) -> str:
    return location.split("/")[0]


def parse_s3_prefix(location: str) -> str:
    return "/".join(location.split("/")[1:-1])


def parse_s3_key(location: str) -> str:
    return "/".join(location.split("/")[1:])


def parse_s3_filename(location: str) -> str:
    return location.split("/")[-1]


def parse_local_path(location: str) -> str:
    return location


def parse_local_dir(location: str) -> str:
    return "/".join(location.split("/")[:-1])


def parse_db_schema(location: str) -> str:
    return location.split(".")[0]


def parse_db_table(location: str) -> str:
    return location.split(".")[1]


def parse_doc_id(location: str) -> str:
    return location.split(".")[0]


def parse_table_id(location: str) -> str:
    return location.split(".")[1]


def parse_iceberg_namespace(location: str) -> str:
    return location.split(".")[0]


def parse_iceberg_table(location: str) -> str:
    return location.split(".")[1]


@dataclass(frozen=True)
class S3DatasetLocationProvider(DatasetLocationProvider):
    conn_id: str

    def fs_config(self, location: str, conn_id: str) -> FSConfig:
        return FSConfig(
            bucket=parse_s3_bucket(location=location),
            connection_id=conn_id,
        )

    def read(
        self,
        location: str,
        read_options: dict[str, Any],
    ) -> pd.DataFrame:
        fs_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=self.fs_config(location=location, conn_id=self.conn_id),
        )
        df = read_dataframe(
            file_handler=fs_handler,
            file_path=parse_s3_key(location),
            read_options=read_options,
        )
        return df

    def write(
        self,
        content: bytes,
        location: str,
    ) -> None:
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=self.fs_config(location=location, conn_id=self.conn_id),
        )
        s3_handler.write(
            file_path=parse_s3_key(location=location),
            content=content,
        )


@dataclass(frozen=True)
class LocalFileDatasetLocationProvider(DatasetLocationProvider):

    def fs_config(self, location: str) -> FSConfig:
        return FSConfig(
            base_path=parse_local_dir(location=location),
        )

    def read(
        self,
        location: str,
        read_options: dict[str, Any],
    ) -> pd.DataFrame:
        fs_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=self.fs_config(location=location),
        )
        df = read_dataframe(
            file_handler=fs_handler,
            file_path=parse_local_path(location=location),
            read_options=read_options,
        )
        return df

    def write(
        self,
        content: bytes,
        location: str,
    ) -> None:
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=self.fs_config(location=location),
        )
        local_handler.write(
            file_path=parse_local_path(location=location),
            content=content,
        )


@dataclass(frozen=True)
class DbDatasetLocationProvider(DatasetLocationProvider):
    conn_id: str

    def read(
        self,
        location: str,
        read_options: dict[str, Any],
    ) -> pd.DataFrame:
        db_config = DbConfig(connection_id=self.conn_id)
        db_handler = create_db_handler(
            db_type=DatabaseType.POSTGRES,
            db_config=db_config,
        )
        schema = parse_db_schema(location=location)
        table = parse_db_table(location=location)
        df = db_handler.fetch_df(
            query=f"SELECT * FROM {schema}.{table}",
        )
        return df

    def write(
        self,
        content: bytes,
        location: str,
    ) -> None:
        """Write content to local filesystem first before bulk import it to Database"""
        local_path = parse_local_path(location=location)
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=FSConfig(
                base_path=parse_local_dir(location=location),
            ),
        )
        local_handler.write(
            file_path=local_path,
            content=content,
        )

        raise NotImplementedError("Database bulk import is not implemented yet")


@dataclass(frozen=True)
class GristDatasetLocationProvider(DatasetLocationProvider):
    def read(
        self,
        location: str,
        read_options: dict[str, Any],
    ) -> pd.DataFrame:
        doc_id = parse_doc_id(location=location)
        table_id = location  # parse_table_id(location=location)
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
        content: bytes,
        location: str,
    ) -> None:
        raise NotImplementedError("GristDatasetWriter is not wired yet")
