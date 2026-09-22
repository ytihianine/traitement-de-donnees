"""Functions for retrieving and managing project configurations."""

import logging

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from modules.constants import DEFAULT_PG_DATA_CONN_ID
from modules.domain.projet.model import (
    Contact,
    Documentation,
    ProjetMetadata,
    ProjetS3,
)
from modules.infra.database.base import DBInterface
from modules.infra.database.exceptions import DatabaseError
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler

CONF_SCHEMA = "conf_projets"
logger = logging.getLogger(name=__name__)


def _get_db(db: DBInterface | None = None) -> DBInterface:
    """Return the provided db handler or create the default one."""
    if db is not None:
        return db
    return create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=DEFAULT_PG_DATA_CONN_ID),
    )


# Configuration du retry decorator
db_retry = retry(
    retry=retry_if_exception_type(exception_types=(ConnectionError, TimeoutError, DatabaseError, OSError)),
    stop=stop_after_attempt(max_attempt_number=3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    reraise=True,
)


@db_retry
def get_list_contact(nom_projet: str, db: DBInterface | None = None) -> list[Contact]:
    if not nom_projet:
        raise ValueError(
            "Variable nom_projet is required to fetch contact information. Current value is None or empty."
        )

    db = _get_db(db)

    query = f"""
        SELECT cppc.projet, cppc.contact_mail, cppc.is_mail_generic
        FROM {CONF_SCHEMA}.projet_contact_vw cppc
        WHERE cppc.projet = %s AND rang = 1;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    records = df.to_dict("records", into=dict)
    return [Contact(**record) for record in records]


@db_retry
def get_list_documentation(
    nom_projet: str,
    db: DBInterface | None = None,
) -> list[Documentation]:
    if not nom_projet:
        raise ValueError("Variable nom_projet is required to fetch documentation. Current value is None or empty.")

    db = _get_db(db)

    query = f"""
        SELECT cppd.projet, cppd.type_documentation, cppd.lien
        FROM {CONF_SCHEMA}.projet_documentation_vw cppd
        WHERE cppd.projet = %s AND rang = 1;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    records = df.to_dict("records", into=dict)
    return [Documentation(**record) for record in records]


@db_retry
def get_projet_s3_info(
    nom_projet: str,
    db: DBInterface | None = None,
) -> ProjetS3:
    """Get S3 configuration for a specific selecteur.

    Args:
        nom_projet: Project name
        db: Optional database handler (defaults to Airflow config connection)

    Returns:
        ProjetS3 object with S3 configuration

    Raises:
        ConfigError: If no S3 configuration is found
    """
    if not nom_projet:
        raise ValueError("Variable nom_projet is required to fetch S3 configuration. Current value is None or empty.")

    db = _get_db(db)

    query = f"""
        SELECT cpps3.projet, cpps3.bucket,
            cpps3.key,
            cpps3.key_tmp
        FROM {CONF_SCHEMA}.projet_s3_vw cpps3
        WHERE cpps3.projet = %s AND rang = 1;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))

    if df.empty:
        raise ValueError(f"No S3 configuration found for project {nom_projet}")

    record = df.iloc[0].to_dict(into=dict)
    return ProjetS3(**record)  # type: ignore


@db_retry
def get_projet_metadata(nom_projet: str, db: DBInterface | None = None, dag_completed: bool = False) -> ProjetMetadata:
    """
    Get the latest completed snapshot for a project.
    """

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

    params = {"nom_projet": nom_projet, "is_dag_completed": dag_completed}

    db = _get_db(db)

    db_result = db.fetch_one(
        query,
        parameters=params,
    )

    if db_result is None:
        raise ValueError(f"No metadata found for project {nom_projet}")

    return ProjetMetadata(
        _id_projet=db_result["id_projet"],
        _snapshot_id=db_result["snapshot_id"],
        _snapshot_id_parent=db_result["snapshot_id_parent"],
        _import_timestamp=db_result["import_timestamp"],
    )
