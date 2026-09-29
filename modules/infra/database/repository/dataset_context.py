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
from modules.domain.dataset.model import Dataset, DatasetContext, DatasetLocation, StageLocation
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
                SELECT
                    "id_projet",
                    "projet",
                    "id_dataset",
                    "dataset",
                    "stage",
                    "id_type_location",
                    "type_location",
                    "location",
                    "id_conn_id",
                    "conn_id",
                FROM {CONF_SCHEMA}.dim_dataset_location cpdd
                WHERE 1=1
                    AND cpdd.projet = %s
                    AND cpdd."import_timestamp" = (
                    SELECT MAX("import_timestamp")
                    FROM conf_projets."dim_dataset_location"
                    WHERE "projet" = %s
                );
            """,
            parameters=(nom_projet, nom_projet),
        )

        if df.empty:
            raise ValueError(f"No project found with name {nom_projet}")

        projet = Projet(name=df.iloc[0]["projet"], id=df.iloc[0]["id_projet"])
        dataset_contexts: list[DatasetContext] = []
        for _, dataset_rows in df.groupby("id_dataset", sort=False):
            dataset = Dataset(name=dataset_rows.iloc[0]["dataset"])
            locations: dict[StageLocation, DatasetLocation] = {}
            for _, row in dataset_rows.iterrows():
                record = row.to_dict(into=dict)
                locations[StageLocation(record["stage"])] = DatasetLocation(
                    type_location=record["type_location"],
                    location=record["location"],
                    conn_id=record["conn_id"],
                )
            dataset_contexts.append(
                DatasetContext(
                    projet=projet,
                    dataset=dataset,
                    location=locations,
                )
            )
        return dataset_contexts

    @db_retry
    def get(self, nom_projet: str, nom_dataset: str) -> DatasetContext:
        df = self.db_client.fetch_df(
            query=f"""
                SELECT
                    "id_projet",
                    "projet",
                    "id_dataset",
                    "dataset",
                    "stage",
                    "id_type_location",
                    "type_location",
                    "location",
                    "id_conn_id",
                    "conn_id"
                FROM {CONF_SCHEMA}.dim_dataset_location cpdd
                WHERE 1=1
                    AND cpdd.projet = %s
                    AND cpdd.dataset = %s
                    AND cpdd."import_timestamp" = (
                    SELECT MAX("import_timestamp")
                    FROM conf_projets."dim_dataset_location"
                    WHERE "projet" = %s
                      AND "dataset" = %s
                );
            """,
            parameters=(nom_projet, nom_dataset, nom_projet, nom_dataset),
        )

        if df.empty:
            raise ValueError(f"No dataset found with name {nom_dataset} in project {nom_projet}")

        projet = Projet(name=df.iloc[0]["projet"], id=df.iloc[0]["id_projet"])
        dataset = Dataset(name=df.iloc[0]["dataset"])
        locations = {}
        for _, row in df.iterrows():
            record = row.to_dict(into=dict)
            loc = DatasetLocation(
                type_location=record["type_location"],
                location=record["location"],
                conn_id=record["conn_id"],
            )
            locations[StageLocation(record["stage"])] = loc
        dataset_context = DatasetContext(
            projet=projet,
            dataset=dataset,
            location=locations,
        )
        return dataset_context

    @db_retry
    def get_list_source_fichier(self, nom_projet: str) -> list[str]:
        source_fichiers = self.db_client.fetch_all(
            query=f"""
                SELECT nom_source AS source_fichier
                FROM {CONF_SCHEMA}.vue_source
                WHERE nom_projet = %s
            """,
            parameters=(nom_projet,),
        )
        return [sf["source_fichier"] for sf in source_fichiers]

    @db_retry
    def get_list_column_mapping(self, nom_projet: str, dataset_name: str) -> Sequence[Mapping[str, Any]]:
        column_mappings = self.db_client.fetch_all(
            query=f"""
                SELECT
                    id_projet,
                    projet AS nom_projet,
                    id_dataset,
                    dataset AS dataset_name,
                    id_col_mapping,
                    colname_source,
                    colname_dest,
                    to_keep,
                    date_archivage,
                    snapshot_id,
                    snapshot_id_parent,
                    import_timestamp
                FROM {CONF_SCHEMA}.dim_dataset_column_mapping
                WHERE projet = %s
                  AND dataset = %s
                  AND import_timestamp = (
                      SELECT MAX(import_timestamp)
                      FROM {CONF_SCHEMA}.dim_dataset_column_mapping
                      WHERE projet = %s
                        AND dataset = %s
                  )
            """,
            parameters=(nom_projet, dataset_name, nom_projet, dataset_name),
        )
        return column_mappings
