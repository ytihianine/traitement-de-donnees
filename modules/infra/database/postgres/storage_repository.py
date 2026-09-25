from dataclasses import dataclass

from modules.domain.dataset.model import StorageInfo
from modules.domain.dataset.ports import StorageInfoProvider
from modules.infra.database.base import DBInterface


@dataclass(frozen=True)
class PostgresStorageRepository(StorageInfoProvider):
    db_client: DBInterface

    def get_by_dataset(self, nom_projet: str, dataset_name: str) -> StorageInfo:
        storage = self.db_client.fetch_one(
            query="SELECT * FROM dataset_storages WHERE nom_projet = %s AND dataset_name = %s",
            parameters=(nom_projet, dataset_name),
        )
        if storage is None:
            raise ValueError(f"Storage info for nom_projet={nom_projet} and dataset_name={dataset_name} not found.")
        return StorageInfo(
            type_source=storage["type_source"],
            id_source=storage["id_source"],
            bucket=storage["bucket"],
            s3_key=storage["s3_key"],
            filename=storage["filename"],
            tbl_name=storage["tbl_name"],
            s3_conn_id=storage["s3_conn_id"],
            local_dir=storage["local_dir"],
        )

    def get_by_projet(self, nom_projet: str) -> list[StorageInfo]:
        storages = self.db_client.fetch_all(
            query="SELECT * FROM dataset_storages WHERE nom_projet = %s",
            parameters=(nom_projet,),
        )
        return [
            StorageInfo(
                type_source=storage["type_source"],
                id_source=storage["id_source"],
                bucket=storage["bucket"],
                s3_key=storage["s3_key"],
                filename=storage["filename"],
                tbl_name=storage["tbl_name"],
                s3_conn_id=storage["s3_conn_id"],
                local_dir=storage["local_dir"],
            )
            for storage in storages
        ]

    def get_list_source_fichier(self, nom_projet: str) -> list[str]:
        sources = self.db_client.fetch_all(
            query="SELECT DISTINCT source_fichier FROM dataset_storages WHERE nom_projet = %s",
            parameters=(nom_projet,),
        )
        return [source["source_fichier"] for source in sources]
