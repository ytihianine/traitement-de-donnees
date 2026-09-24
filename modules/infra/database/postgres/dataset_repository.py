import logging

import pandas as pd
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from modules.domain.dataset.model import Dataset, DatasetStorage
from modules.domain.dataset.repository import DatasetRepository, DatasetStorageRepository
from modules.infra.database.base import DBInterface
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
    db_client: DBInterface

    def get(self, id_projet: int, name: str) -> Dataset:
        dataset = self.db_client.fetch_one(
            query="SELECT * FROM datasets WHERE id_projet = %s AND name = %s", parameters=(id_projet, name)
        )
        if dataset is None:
            raise ValueError(f"Dataset with id_projet={id_projet} and name={name} not found.")
        return Dataset(**dataset)

    def get_list(self, id_projet: int) -> list[Dataset]:
        datasets = self.db_client.fetch_all(
            query="SELECT * FROM datasets WHERE id_projet = %s", parameters=(id_projet,)
        )
        return [Dataset(**dataset) for dataset in datasets]


class PostgresDatasetStorageRepository(DatasetStorageRepository):
    db_client: DBInterface

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

    def get(self, dataset: Dataset) -> DatasetStorage:
        storage = self.db_client.fetch_one(
            query="SELECT * FROM dataset_storages WHERE id_dataset = %s", parameters=(dataset.id,)
        )
        if storage is None:
            raise ValueError(f"DatasetStorage for dataset id={dataset.id} not found.")
        return DatasetStorage(**storage)

    def get_list(self, id_projet: int) -> list[DatasetStorage]:
        storages = self.db_client.fetch_all(
            query="SELECT * FROM dataset_storages WHERE id_projet = %s", parameters=(id_projet,)
        )
        return [DatasetStorage(**storage) for storage in storages]

    def get_list_source_fichier(self, id_projet: int) -> list[str]:
        sources = self.db_client.fetch_all(
            query="SELECT DISTINCT source_fichier FROM dataset_storages WHERE id_projet = %s", parameters=(id_projet,)
        )
        return [source["source_fichier"] for source in sources]

    def get_list_column_mapping_as_df(self, id_projet: int, selecteur: str) -> pd.DataFrame:
        column_mappings = self.db_client.fetch_all(
            query="SELECT * FROM column_mappings WHERE id_projet = %s AND selecteur = %s",
            parameters=(id_projet, selecteur),
        )
        return pd.DataFrame(column_mappings)
