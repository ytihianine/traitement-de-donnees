import logging
from collections.abc import Mapping, Sequence
from typing import Any

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from modules.constants import DEFAULT_PG_DATA_CONN_ID
from modules.domain.dataset.model import Dataset
from modules.domain.dataset.repository import DatasetRepository
from modules.infra.database.base import DBInterface
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.database.postgres.projet_repository import CONF_SCHEMA

logger = logging.getLogger(name=__name__)
db_retry = retry(
    retry=retry_if_exception_type(exception_types=(ConnectionError, TimeoutError, OSError)),
    stop=stop_after_attempt(max_attempt_number=3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    reraise=True,
)


# =================
# Dataclasses
# =================
class PostgresDatasetRepository(DatasetRepository):
    """ProjetRepository backed by the ``conf_projets`` Postgres schema."""

    db_type: DatabaseType = DatabaseType.POSTGRES
    db_connection_id: str = DEFAULT_PG_DATA_CONN_ID

    @property
    def db_client(self) -> DBInterface:
        client = create_db_handler(
            db_type=self.db_type,
            db_config=DbConfig(connection_id=self.db_connection_id),
        )
        return client

    def get(self, nom_projet: str, name: str) -> Dataset:
        dataset = self.db_client.fetch_one(
            query="SELECT * FROM datasets WHERE nom_projet = %s AND name = %s", parameters=(nom_projet, name)
        )
        if dataset is None:
            raise ValueError(f"Dataset with nom_projet={nom_projet} and name={name} not found.")
        return Dataset(nom_projet=nom_projet, name=dataset["name"])

    def get_list(self, nom_projet: str) -> list[Dataset]:
        results = self.db_client.fetch_all(
            query="SELECT * FROM datasets WHERE nom_projet = %s", parameters=(nom_projet,)
        )
        datasets = []
        for dataset in results:
            datasets.append(Dataset(nom_projet=nom_projet, name=dataset["name"]))
        return datasets

    @db_retry
    def _get_selecteur_storage_info(
        self,
        nom_projet: str,
        selecteur: str | None = None,
        only_source: bool = False,
        only_grist: bool = False,
        only_fichier: bool = False,
    ) -> list[Dataset]:
        """Get Dataset storage info for a project and optionally a specific selecteur.

        Args:
            nom_projet: Project name
            selecteur: Optional selecteur filter

        Returns:
            List of Dataset objects
        """
        if not nom_projet:
            raise ValueError(
                "Variable nom_projet is required to fetch selecteur storage info. Current value is None or empty."
            )

        db = self.db_client

        query = f"""
            SELECT cpss3db.projet, cpss3db.selecteur, cpss3db.type_source, cpss3db.id_source,
                cpss3db.bucket, cpss3db.s3_key, cpss3db.filename,
                cpss3db.tbl_name
            FROM {CONF_SCHEMA}.selecteur_s3_db_vw cpss3db
            WHERE 1=1 AND cpss3db.projet = %s AND rang = 1
        """

        if only_source:
            query += " AND cpss3db.type_selecteur = 'Source'"

        if only_grist:
            query += " AND cpss3db.type_source = 'Grist'"

        if only_fichier:
            query += " AND cpss3db.type_source = 'Fichier'"

        params: list[str] = [nom_projet]

        if selecteur is not None:
            query += " AND cpss3db.selecteur = %s"
            params.append(selecteur)

        query += " ORDER BY cpss3db.projet;"

        df = db.fetch_df(query, parameters=tuple(params))

        if df.empty:
            return []

        records = df.to_dict("records", into=dict)
        return [Dataset(**record) for record in records]

    def get_list_source_fichier(self, nom_projet: str) -> list[str]:
        sources = self.db_client.fetch_all(
            query="SELECT DISTINCT source_fichier FROM dataset_storages WHERE nom_projet = %s", parameters=(nom_projet,)
        )
        return [source["source_fichier"] for source in sources]

    def get_list_column_mapping(self, nom_projet: str, dataset_name: str) -> Sequence[Mapping[str, Any]]:
        column_mappings = self.db_client.fetch_all(
            query="SELECT * FROM column_mappings WHERE nom_projet = %s AND dataset_name = %s",
            parameters=(nom_projet, dataset_name),
        )
        return column_mappings
