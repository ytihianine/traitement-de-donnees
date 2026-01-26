"""Functions for retrieving and managing project configurations."""

from dataclasses import is_dataclass
from typing import Any, Mapping, Optional
import logging

from attr import asdict
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import pandas as pd

from utils.config.dag_params import get_project_name
from _types.projet import (
    Contact,
    DbInfo,
    Documentation,
    ProjetS3,
    SelecteurInfo,
    SelecteurS3,
    SourceFichier,
    SourceGrist,
)
from utils.exceptions import ConfigError
from infra.database.factory import create_db_handler
from utils.config.vars import DEFAULT_PG_CONFIG_CONN_ID

CONF_SCHEMA = "conf_projets"
logger = logging.getLogger(name=__name__)

# Configuration du retry decorator
db_retry = retry(
    retry=retry_if_exception_type(
        exception_types=(ConnectionError, TimeoutError, Exception)
    ),
    stop=stop_after_attempt(max_attempt_number=3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    reraise=True,
)


@db_retry
def column_mapping_dataframe(
    nom_projet: str,
    selecteur: str,
) -> pd.DataFrame:
    """
    Permet de récupérer la correspondance des colonnes entre
    le fichier source et la sortie.
    Output:
        Dataframe
        Columns: selecteur, colname_source, colname_dest
    """
    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    df = db.fetch_df(
        query=f"""SELECT cpvcm.nom_projet, cpvcm.selecteur, cpvcm.colname_source, cpvcm.colname_dest
            FROM {CONF_SCHEMA}.vue_cols_mapping cpvcm
            WHERE cpvcm.nom_projet = %s AND cpvcm.selecteur = %s;
        """,
        parameters=(nom_projet, selecteur),
    )
    return df


def column_mapping_dict(
    df_cols_map: pd.DataFrame, selecteur: Optional[str] = None
) -> dict[str, str]:
    print("Colonnes du dataframe de mapping: ", df_cols_map.columns)
    print("Selecteurs du dataframe de mapping: ", df_cols_map["selecteur"].unique())
    if selecteur is not None:
        df_cols_map = df_cols_map.loc[df_cols_map["selecteur"] == selecteur]
    records_cols_map = df_cols_map.to_dict("records")
    cols_map = {}
    for record in records_cols_map:
        cols_map[record["colname_source"]] = record["colname_dest"]

    return cols_map


@db_retry
def _get_db_info_tbl_names(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    selecteur: str | None = None,
) -> list[DbInfo]:
    """Get database info for a project and optionally a specific selecteur.

    Args:
        context: Airflow task context
        nom_projet: Project name
        selecteur: Optional selecteur filter

    Returns:
        List of DbInfo objects ordered by tbl_order
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context is not None:
        nom_projet = get_project_name(context=context)

    if nom_projet is None:
        raise ValueError("Could not determine project name from context")

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT cppd.projet, cppd.selecteur, cppd.tbl_name, cppd.tbl_order,
               cppd.is_partitionned, cppd.partition_period, cppd.load_strategy
        FROM {CONF_SCHEMA}.projet_database_vw cppd
        WHERE cppd.tbl_name IS NOT NULL
            AND cppd.tbl_name <> ''
            AND cppd.projet = %s
    """

    params = [nom_projet]

    if selecteur is not None:
        query += " AND cppd.selecteur = %s"
        params.append(selecteur)

    query += " ORDER BY cppd.tbl_order;"

    df = db.fetch_df(query, parameters=tuple(params))

    if df.empty:
        return []

    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [DbInfo(**record) for record in records]


def get_list_table_name(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[str]:
    """Get all table names for a project.

    Used for temporary table creation.

    Args:
        context: Airflow task context
        nom_projet: Project name

    Returns:
        List of table names ordered by tbl_order
    """
    db_infos = _get_db_info_tbl_names(
        context=context, nom_projet=nom_projet, selecteur=None
    )
    return [db_info.tbl_name for db_info in db_infos]


def get_table_name(
    nom_projet: str, selecteur: str, context: Mapping[str, Any] | None = None
) -> str:
    """Get table name for a specific selecteur.

    Args:
        nom_projet: Project name
        selecteur: Selecteur name
        context: Optional Airflow task context

    Returns:
        Table name for the specified selecteur

    Raises:
        ConfigError: If no table name is found
    """
    db_infos = _get_db_info_tbl_names(
        context=context, nom_projet=nom_projet, selecteur=selecteur
    )

    if not db_infos:
        raise ConfigError(
            message=f"No table name found for project {nom_projet} and selecteur {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
        )

    return db_infos[0].tbl_name


@db_retry
def get_source_grist(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    selecteur: str | None = None,
) -> SourceGrist:
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if selecteur is None:
        raise ValueError("selecteur must be provided in the args")

    if nom_projet is None and context:
        nom_projet = get_project_name(context=context)

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT cpssg.projet, cpssg.selecteur, cpssg.type_source, cpssg.id_source,
            cpssg.filename,
            cpssg.s3_key,
            cpssg.bucket,
            cpssg.projet_s3_key,
            cpssg.projet_s3_key_tmp,
            cpssg.filepath_s3,
            cpssg.filepath_tmp_s3
        FROM {CONF_SCHEMA}.selecteur_source_grist_vw cpssg
        WHERE cpssg.projet = %s AND cpssg.selecteur = %s;
    """

    df = db.fetch_df(query, parameters=(nom_projet, selecteur))
    records = df.to_dict("records")
    return SourceGrist(**{str(k): v for k, v in records[0].items()})


@db_retry
def get_list_contact(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[Contact]:
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context:
        nom_projet = get_project_name(context=context)

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = """
        SELECT cppc.projet, cppc.contact_mail, cppc.is_mail_generic
        FROM {CONF_SCHEMA}.projet_contact_vw cppc
        WHERE cppc.projet = %s;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [Contact(**record) for record in records]


@db_retry
def get_list_documentation(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[Documentation]:
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context:
        nom_projet = get_project_name(context=context)

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = """
        SELECT cppd.projet, cppd.type_documentation, cppd.lien
        FROM {CONF_SCHEMA}.projet_documentation_vw cppd
        WHERE cppd.projet = %s;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [Documentation(**record) for record in records]


@db_retry
def _get_db_info(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    selecteur: str | None = None,
) -> list[DbInfo]:
    """Get database info for a project and optionally a specific selecteur.

    Args:
        context: Airflow task context
        nom_projet: Project name
        selecteur: Optional selecteur filter

    Returns:
        List of DbInfo objects
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context is not None:
        nom_projet = get_project_name(context=context)

    if nom_projet is None:
        raise ValueError("Could not determine project name from context")

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT cppd.projet, cppd.selecteur, cppd.tbl_name,
            cppd.tbl_order, cppd.is_partitionned, cppd.partition_period,
            cppd.load_strategy
        FROM {CONF_SCHEMA}.projet_database_vw cppd
        WHERE cppd.projet = %s
    """

    params = [nom_projet]

    if selecteur is not None:
        query += " AND cppd.selecteur = %s"
        params.append(selecteur)

    query += "ORDER BY cppd.tbl_order ASC;"

    df = db.fetch_df(query, parameters=tuple(params))
    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [DbInfo(**record) for record in records]


def get_list_database_info(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[DbInfo]:
    """Get all database info for a project.

    Args:
        context: Airflow task context
        nom_projet: Project name

    Returns:
        List of DbInfo objects for all selecteurs in the project
    """
    return _get_db_info(context=context, nom_projet=nom_projet, selecteur=None)


def get_database_info(
    nom_projet: str, selecteur: str, context: Mapping[str, Any] | None = None
) -> DbInfo:
    """Get database info for a specific selecteur.

    Args:
        nom_projet: Project name
        selecteur: Selecteur name
        context: Optional Airflow task context

    Returns:
        DbInfo object for the specified selecteur

    Raises:
        ConfigError: If no configuration is found
    """
    db_infos = _get_db_info(context=context, nom_projet=nom_projet, selecteur=selecteur)

    if not db_infos:
        raise ConfigError(
            message=f"No database configuration found for project {nom_projet} and selecteur {selecteur}",  # noqa
            nom_projet=nom_projet,
            selecteur=selecteur,
        )

    return db_infos[0]


@db_retry
def _get_source_fichier(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    selecteur: str | None = None,
) -> list[SourceFichier]:
    """Get source fichier info for a project and optionally a specific selecteur.

    Args:
        context: Airflow task context
        nom_projet: Project name
        selecteur: Optional selecteur filter

    Returns:
        List of SourceFichier objects
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context is not None:
        nom_projet = get_project_name(context=context)

    if nom_projet is None:
        raise ValueError("Could not determine project name from context")

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT
            cssf.projet,
            cssf.selecteur,
            cssf.bucket,
            cssf.s3_key,
            -- Source
            cssf.type_source,
            cssf.id_source,
            cssf.filepath_source_s3,
            -- Destination
            cssf.filename,
            cssf.projet_s3_key,
            cssf.projet_s3_key_tmp,
            cssf.filepath_s3,
            cssf.filepath_tmp_s3
        FROM {CONF_SCHEMA}.selecteur_source_fichier_vw cssf
        WHERE cssf.projet = %s
    """

    params = [nom_projet]

    if selecteur is not None:
        query += " AND cssf.selecteur = %s"
        params.append(selecteur)

    query += ";"

    df = db.fetch_df(query, parameters=tuple(params))
    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [SourceFichier(**record) for record in records]


def get_list_source_fichier(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[SourceFichier]:
    """Get all source fichier info for a project.

    Args:
        context: Airflow task context
        nom_projet: Project name

    Returns:
        List of SourceFichier objects for all selecteurs in the project
    """
    return _get_source_fichier(context=context, nom_projet=nom_projet, selecteur=None)


def get_list_source_fichier_key(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[str]:
    """Get all source fichier s3_key for a project.

    Args:
        context: Airflow task context
        nom_projet: Project name

    Returns:
        List of SourceFichier objects for all selecteurs in the project
    """
    source_fichiers = _get_source_fichier(
        context=context, nom_projet=nom_projet, selecteur=None
    )
    return [source.filepath_source_s3 for source in source_fichiers]


def get_source_fichier(
    nom_projet: str, selecteur: str, context: Mapping[str, Any] | None = None
) -> SourceFichier:
    """Get source fichier info for a specific selecteur.

    Args:
        nom_projet: Project name
        selecteur: Selecteur name
        context: Optional Airflow task context

    Returns:
        SourceFichier object for the specified selecteur

    Raises:
        ConfigError: If no configuration is found
    """
    sources = _get_source_fichier(
        context=context, nom_projet=nom_projet, selecteur=selecteur
    )

    if not sources:
        raise ConfigError(
            message=f"No source fichier found for project {nom_projet} and selecteur {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
        )

    return sources[0]


@db_retry
def _get_selecteur_s3(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    selecteur: str | None = None,
) -> list[SelecteurS3]:
    """Get S3 configuration for a project and optionally a specific selecteur.

    Args:
        context: Airflow task context
        nom_projet: Project name
        selecteur: Optional selecteur filter

    Returns:
        List of SelecteurS3 objects
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context is not None:
        nom_projet = get_project_name(context=context)

    if nom_projet is None:
        raise ValueError("Could not determine project name from context")

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT cpss3.projet, cpss3.selecteur, cpss3.filename,
            cpss3.s3_key, cpss3.bucket, cpss3.projet_s3_key,
            cpss3.projet_s3_key_tmp, cpss3.filepath_s3, cpss3.filepath_tmp_s3
        FROM {CONF_SCHEMA}.selecteur_s3_vw cpss3
        WHERE cpss3.projet = %s
    """

    params = [nom_projet]

    if selecteur is not None:
        query += " AND cpss3.selecteur = %s"
        params.append(selecteur)

    query += ";"

    df = db.fetch_df(query, parameters=tuple(params))

    if df.empty:
        return []

    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [SelecteurS3(**record) for record in records]


def get_projet_selecteur_s3(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[SelecteurS3]:
    """Get all S3 configurations for a project.

    Args:
        context: Airflow task context
        nom_projet: Project name

    Returns:
        List of SelecteurS3 objects for all selecteurs in the project
    """
    return _get_selecteur_s3(context=context, nom_projet=nom_projet, selecteur=None)


def get_selecteur_s3(
    selecteur: str,
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
) -> SelecteurS3:
    """Get S3 configuration for a specific selecteur.

    Args:
        selecteur: Selecteur name
        context: Optional Airflow task context
        nom_projet: Project name

    Returns:
        SelecteurS3 object with S3 configuration

    Raises:
        ConfigError: If no S3 configuration is found
    """
    s3_configs = _get_selecteur_s3(
        context=context, nom_projet=nom_projet, selecteur=selecteur
    )

    if not s3_configs:
        raise ConfigError(
            message=f"No S3 configuration found for project {nom_projet} and selecteur {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
        )

    return s3_configs[0]


@db_retry
def get_projet_s3_info(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
) -> ProjetS3:
    """Get S3 configuration for a specific selecteur.

    Args:
        context: Optional Airflow task context
        nom_projet: Project name

    Returns:
        ProjetS3 object with S3 configuration

    Raises:
        ConfigError: If no S3 configuration is found
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context is not None:
        nom_projet = get_project_name(context=context)

    if nom_projet is None:
        raise ValueError("Could not determine project name from context")

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT cpps3.projet, cpps3.bucket,
            cpps3.key,
            cpps3.key_tmp
        FROM {CONF_SCHEMA}.projet_s3_vw cpps3
        WHERE cpps3.projet = %s;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))

    if df.empty:
        raise ConfigError(
            message=f"No S3 configuration found for project {nom_projet}",
            nom_projet=nom_projet,
        )

    record = df.iloc[0].to_dict()
    record = {str(k): v for k, v in record.items()}
    return ProjetS3(**record)


@db_retry
def _get_selector_info(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    selecteur: str | None = None,
) -> list[SelecteurInfo]:
    """Get S3 configuration for a project and optionally a specific selecteur.

    Args:
        context: Airflow task context
        nom_projet: Project name
        selecteur: Optional selecteur filter

    Returns:
        List of SelecteurS3 objects
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context is not None:
        nom_projet = get_project_name(context=context)

    if nom_projet is None:
        raise ValueError("Could not determine project name from context")

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT cpss3db.projet, cpss3db.selecteur, cpss3db.filename,
            cpss3db.s3_key, cpss3db.bucket, cpss3db.projet_s3_key,
            cpss3db.projet_s3_key_tmp, cpss3db.filepath_s3,
            cpss3db.filepath_tmp_s3,
            cpss3db.tbl_name, cpss3db.tbl_order,
            cpss3db.is_partitionned, cpss3db.partition_period,
            cpss3db.load_strategy
        FROM {CONF_SCHEMA}.selecteur_s3_db_vw cpss3db
        WHERE cpss3db.projet = %s
    """

    params = [nom_projet]

    if selecteur is not None:
        query += " AND cpss3db.selecteur = %s"
        params.append(selecteur)

    query += "ORDER BY cpss3db.projet, cpss3db.tbl_order;"

    df = db.fetch_df(query, parameters=tuple(params))

    if df.empty:
        return []

    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [SelecteurInfo(**record) for record in records]


def get_list_selector_info(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[SelecteurInfo]:
    """Get all S3 configurations for a project.

    Args:
        context: Airflow task context
        nom_projet: Project name

    Returns:
        List of SelecteurS3 objects for all selecteurs in the project
    """
    return _get_selector_info(context=context, nom_projet=nom_projet, selecteur=None)


def get_selector_info(
    selecteur: str,
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
) -> SelecteurInfo:
    """Get S3 configuration for a specific selecteur.

    Args:
        selecteur: Selecteur name
        context: Optional Airflow task context
        nom_projet: Project name

    Returns:
        SelecteurS3 object with S3 configuration

    Raises:
        ConfigError: If no S3 configuration is found
    """
    s3_configs = _get_selector_info(
        context=context, nom_projet=nom_projet, selecteur=selecteur
    )

    if not s3_configs:
        raise ConfigError(
            message=f"No S3 configuration found for project {nom_projet} and selecteur {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
        )

    return s3_configs[0]
