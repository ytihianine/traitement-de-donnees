"""PostgreSQL adapter for the ProjetRepository port."""

import logging
from dataclasses import dataclass

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from modules.constants import DEFAULT_PG_DATA_CONN_ID
from modules.domain.projet.model import Contact, Documentation, Projet, ProjetMetadata, ProjetS3
from modules.domain.projet.repository import ProjetRepository
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
class PostgresProjetRepository(ProjetRepository):
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

    @db_retry
    def get(self, nom_projet: str) -> Projet:
        df = self.db_client.fetch_df(
            query=f"""
                SELECT p.projet, p.id_projet
                FROM {CONF_SCHEMA}.projet p
                JOIN versioning.snapshot s ON p.id_projet = s.id_projet
                WHERE p.projet = %s
                ORDER BY s.import_timestamp DESC
                LIMIT 1;
            """,
            parameters=(nom_projet,),
        )

        if df.empty:
            raise ValueError(f"No project found with name {nom_projet}")

        record = df.iloc[0].to_dict(into=dict)
        return Projet(name=record["projet"], id=record["id_projet"])

    @db_retry
    def get_list_contact(self, nom_projet: str) -> list[Contact]:

        df = self.db_client.fetch_df(
            query=f"""
                SELECT cppc.projet, cppc.contact_mail, cppc.is_mail_generic
                FROM {CONF_SCHEMA}.projet_contact_vw cppc
                WHERE cppc.projet = %s AND rang = 1;
            """,
            parameters=(nom_projet,),
        )
        records = df.to_dict("records", into=dict)
        return [Contact(**record) for record in records]

    @db_retry
    def get_list_documentation(self, nom_projet: str) -> list[Documentation]:

        df = self.db_client.fetch_df(
            query=f"""
                SELECT cppd.projet, cppd.type_documentation, cppd.lien
                FROM {CONF_SCHEMA}.projet_documentation_vw cppd
                WHERE cppd.projet = %s AND rang = 1;
            """,
            parameters=(nom_projet,),
        )
        records = df.to_dict("records", into=dict)
        return [Documentation(**record) for record in records]

    @db_retry
    def get_projet_s3_info(self, nom_projet: str) -> ProjetS3:

        df = self.db_client.fetch_df(
            query=f"""
                SELECT cpps3.projet, cpps3.bucket,
                    cpps3.key,
                    cpps3.key_tmp
                FROM {CONF_SCHEMA}.projet_s3_vw cpps3
                WHERE cpps3.projet = %s AND rang = 1;
            """,
            parameters=(nom_projet,),
        )

        if df.empty:
            raise ValueError(f"No S3 configuration found for project {nom_projet}")

        record = df.iloc[0].to_dict(into=dict)
        return ProjetS3(**record)  # type: ignore[arg-type]

    @db_retry
    def get_projet_metadata(self, nom_projet: str, dag_completed: bool = False) -> ProjetMetadata:

        query = """
            SELECT s.id_projet, s.snapshot_id, s.snapshot_id_parent, s.import_timestamp
            FROM versioning.snapshot s
            JOIN conf_projets.projet p
                ON p.id_projet = s.id_projet
            WHERE p.projet = %(nom_projet)s
              AND s.is_dag_completed = %(is_dag_completed)s
            ORDER BY s.import_timestamp DESC
            LIMIT 1;
        """

        db_result = self.db_client.fetch_one(
            query,
            parameters={"nom_projet": nom_projet, "is_dag_completed": dag_completed},
        )

        if db_result is None:
            raise ValueError(f"No metadata found for project {nom_projet}")

        return ProjetMetadata(
            id_projet=db_result["id_projet"],
            snapshot_id=db_result["snapshot_id"],
            snapshot_id_parent=db_result["snapshot_id_parent"],
            import_timestamp=db_result["import_timestamp"],
        )
