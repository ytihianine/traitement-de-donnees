import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from modules.domain.dataset.ports import DatasetLocationProvider
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler

GRIST_SQLITE_PATH_READ_OPTION = "grist_sqlite_path"


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
        """Write parquet bytes to a local TSV file, then bulk insert with COPY."""
        schema = parse_db_schema(location=location)
        tbl_name = parse_db_table(location=location)

        # DataFrame outputs are serialized as parquet bytes upstream.
        dataframe = pd.read_parquet(path=io.BytesIO(initial_bytes=content))

        local_dir = Path("/tmp")
        local_path = local_dir / f"{schema}_{tbl_name}.tsv"
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=FSConfig(
                base_path=str(local_dir),
            ),
        )

        tsv_content = dataframe.to_csv(
            sep="\t",
            index=False,
            na_rep="NULL",
        ).encode(encoding="utf-8")
        local_handler.write(
            file_path=local_path,
            content=tsv_content,
        )

        db_handler = create_db_handler(
            db_type=DatabaseType.POSTGRES,
            db_config=DbConfig(connection_id=self.conn_id),
        )
        copy_sql = f"""
            COPY {schema}.{tbl_name} ({", ".join(dataframe.columns)})
            FROM STDIN WITH (
                FORMAT TEXT,
                DELIMITER E'\t',
                HEADER TRUE,
                NULL 'NULL'
            )
        """
        db_handler.copy_expert(
            sql=copy_sql,
            filepath=str(local_path),
        )


@dataclass(frozen=True)
class GristDatasetLocationProvider(DatasetLocationProvider):
    def read(
        self,
        location: str,
        read_options: dict[str, Any],
    ) -> pd.DataFrame:
        doc_local_path = Path(
            read_options.get(
                GRIST_SQLITE_PATH_READ_OPTION,
                Path("/tmp") / f"{parse_doc_id(location=location)}.sqlite",
            )
        )
        table_id = location  # parse_table_id(location=location)

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
