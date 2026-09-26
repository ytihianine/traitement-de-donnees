"""PostgreSQL adapter for the ProjetRepository port."""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from modules.constants import DEFAULT_PG_DATA_CONN_ID
from modules.domain.dataset.model import Dataset, DatasetContext, StorageInfo
from modules.domain.dataset.repository import DatasetContextRepository
from modules.domain.projet.model import Projet
from modules.infra.database.base import DBInterface
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler

logger = logging.getLogger(name=__name__)

CONF_SCHEMA = "conf_projets"

db_retry = retry(
    retry=retry_if_exception_type(exception_types=(ConnectionError, TimeoutError, OSError)),
    stop=stop_after_attempt(max_attempt_number=3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    reraise=True,
)


@dataclass(frozen=True)
class DbDatasetContextRepository(DatasetContextRepository):
    """DatasetContextRepository backed by the ``conf_projets`` Postgres schema."""

    db_type: DatabaseType = DatabaseType.POSTGRES
    db_connection_id: str = DEFAULT_PG_DATA_CONN_ID

    @property
    def db_client(self) -> DBInterface:
        client = create_db_handler(
            db_type=self.db_type,
            db_config=DbConfig(connection_id=self.db_connection_id),
        )
        return client

    @db_retry
    def get_list(self, nom_projet: str) -> list[DatasetContext]:
        df = self.db_client.fetch_df(
            query=f"""
                SELECT p.projet, p.id_projet, p.dataset_name, p.s3_conn_id, p.bucket, p.s3_key, p.filename, p.local_dir, p.db_conn_id, p.tbl_name, p.type_source, p.id_source
                FROM {CONF_SCHEMA}.projet p
                WHERE p.projet = %s AND p.rang = 1
                ORDER BY p.import_timestamp DESC;
            """,
            parameters=(nom_projet,),
        )

        if df.empty:
            raise ValueError(f"No project found with name {nom_projet}")

        dataset_contexts = []
        for _, row in df.iterrows():
            record = row.to_dict(into=dict)
            dataset_context = DatasetContext(
                projet=Projet(name=record["projet"], id=record["id_projet"]),
                dataset=Dataset(name=record["dataset_name"]),
                storage_info=StorageInfo(
                    s3_conn_id=record["s3_conn_id"],
                    bucket=record["bucket"],
                    s3_key=record["s3_key"],
                    filename=record["filename"],
                    local_dir=record["local_dir"],
                    db_conn_id=record["db_conn_id"],
                    tbl_name=record["tbl_name"],
                    type_source=record["type_source"],
                    id_source=record["id_source"],
                ),
            )
            dataset_contexts.append(dataset_context)
        return dataset_contexts

    @db_retry
    def get(self, nom_projet: str, nom_dataset: str) -> DatasetContext:
        df = self.db_client.fetch_df(
            query=f"""
                SELECT p.projet, p.id_projet, p.dataset_name, p.s3_conn_id, p.bucket, p.s3_key, p.filename, p.local_dir, p.db_conn_id, p.tbl_name, p.type_source, p.id_source
                FROM {CONF_SCHEMA}.projet p
                WHERE p.projet = %s AND p.dataset_name = %s
                ORDER BY p.import_timestamp DESC
                LIMIT 1;
            """,
            parameters=(nom_projet, nom_dataset),
        )

        if df.empty:
            raise ValueError(f"No project found with name {nom_projet} and dataset {nom_dataset}")

        record = df.iloc[0].to_dict(into=dict)
        dataset_context = DatasetContext(
            projet=Projet(name=record["projet"], id=record["id_projet"]),
            dataset=Dataset(name=nom_dataset),
            storage_info=StorageInfo(
                s3_conn_id=record["s3_conn_id"],
                bucket=record["bucket"],
                s3_key=record["s3_key"],
                filename=record["filename"],
                local_dir=record["local_dir"],
                db_conn_id=record["db_conn_id"],
                tbl_name=record["tbl_name"],
                type_source=record["type_source"],
                id_source=record["id_source"],
            ),
        )
        return dataset_context

    @db_retry
    def get_list_source_fichier(self, nom_projet: str) -> list[str]:
        source_fichiers = self.db_client.fetch_all(
            query="SELECT * FROM source_fichiers WHERE nom_projet = %s",
            parameters=(nom_projet,),
        )
        return [sf["source_fichier"] for sf in source_fichiers]

    @db_retry
    def get_list_column_mapping(self, nom_projet: str, dataset_name: str) -> Sequence[Mapping[str, Any]]:
        column_mappings = self.db_client.fetch_all(
            query="SELECT * FROM column_mappings WHERE nom_projet = %s AND dataset_name = %s",
            parameters=(nom_projet, dataset_name),
        )
        return column_mappings
