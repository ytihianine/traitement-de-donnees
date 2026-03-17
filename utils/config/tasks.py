"""Functions for retrieving and managing project configurations."""

from typing import Any, Mapping, Optional
import logging

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
    Documentation,
    ProjetS3,
    SelecteurStorageInfo,
    SelecteurStorageOptions,
    SelecteurConfig,
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
        query=f"""SELECT cpcm.projet, cpcm.selecteur, cpcm.colname_source, cpcm.colname_dest
            FROM {CONF_SCHEMA}.cols_mapping_vw cpcm
            WHERE cpcm.projet = %s AND cpcm.selecteur = %s;
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
def get_list_contact(
    context: Mapping[str, Any] | None = None, nom_projet: str | None = None
) -> list[Contact]:
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context:
        nom_projet = get_project_name(context=context)

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
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

    query = f"""
        SELECT cppd.projet, cppd.type_documentation, cppd.lien
        FROM {CONF_SCHEMA}.projet_documentation_vw cppd
        WHERE cppd.projet = %s;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    records = df.to_dict("records")
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [Documentation(**record) for record in records]


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


def merge_selecteur_config(
    selecteur_info: list[SelecteurStorageInfo],
    options_map: Mapping[str, SelecteurStorageOptions] | None = None,
) -> list[SelecteurConfig]:
    """Merge SelecteurStorageInfo from DB with local SelecteurStorageOptions config.

    Items like S3 paths and table names come from the DB (via ``selecteur_info``).
    Per-selecteur behavioural options (write flags, load strategy, partition
    period …) are supplied locally as a plain Python dict so that callers can
    use Enum values directly.  Any selecteur that is not present in
    ``options_map`` falls back to the ``SelecteurStorageOptions`` defaults.

    Args:
        selecteur_info: List of SelecteurStorageInfo objects retrieved from the DB.
        options_map: Optional mapping of selecteur name → SelecteurStorageOptions.
                     Selecteurs absent from the map receive default options.

    Returns:
        List of SelecteurConfig objects combining DB info with local options.
    """
    if options_map is None:
        options_map = {}

    return [
        SelecteurConfig.load(
            selecteur_info=info,
            options=options_map.get(info.selecteur, SelecteurStorageOptions()),
        )
        for info in selecteur_info
    ]


@db_retry
def _get_selecteur_storage_info(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    selecteur: str | None = None,
    local_dir: str = "/tmp",
    only_source: bool = False,
    only_grist: bool = False,
    only_fichier: bool = False,
) -> list[SelecteurStorageInfo]:
    """Get SelecteurStorageInfo for a project and optionally a specific selecteur.

    Args:
        context: Airflow task context
        nom_projet: Project name
        selecteur: Optional selecteur filter
        local_dir: Local directory used to build local_path (default: /tmp)

    Returns:
        List of SelecteurStorageInfo objects
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")

    if nom_projet is None and context is not None:
        nom_projet = get_project_name(context=context)

    if nom_projet is None:
        raise ValueError("Could not determine project name from context")

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT cpss3db.projet, cpss3db.selecteur,
            cpss3db.bucket, cpss3db.s3_key, COALESCE(cpss3db.filename, 'filename_undefined') AS filename,
            COALESCE(cpss3db.tbl_name, 'tbl_name_undefined') AS tbl_name
        FROM {CONF_SCHEMA}.selecteur_s3_db_vw cpss3db
        WHERE 1=1 AND cpss3db.projet = %s
    """

    if only_source:
        query += " AND cpss3db.is_source = true"

    if only_grist:
        query += " AND cpss3db.type_source = 'grist'"

    if only_fichier:
        query += " AND cpss3db.type_source = 'file'"

    params: list[str] = [nom_projet]

    if selecteur is not None:
        query += " AND cpss3db.selecteur = %s"
        params.append(selecteur)

    query += " ORDER BY cpss3db.projet, cpss3db.tbl_order;"

    df = db.fetch_df(query, parameters=tuple(params))

    if df.empty:
        return []

    records = df.to_dict("records")
    return [
        SelecteurStorageInfo(
            **{str(k): v for k, v in record.items()}, local_dir=local_dir
        )
        for record in records
    ]


def get_list_selecteur_config(
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    local_dir: str = "/tmp",
) -> list[SelecteurStorageInfo]:
    """Get SelecteurStorageInfo for all selecteurs in a project.

    Args:
        context: Airflow task context
        nom_projet: Project name
        local_dir: Local directory used to build local_path (default: /tmp)

    Returns:
        List of SelecteurStorageInfo objects for all selecteurs
    """
    return _get_selecteur_storage_info(
        context=context, nom_projet=nom_projet, selecteur=None, local_dir=local_dir
    )


def get_selecteur_config(
    selecteur: str,
    context: Mapping[str, Any] | None = None,
    nom_projet: str | None = None,
    local_dir: str = "/tmp",
) -> SelecteurStorageInfo:
    """Get SelecteurStorageInfo for a specific selecteur.

    Args:
        selecteur: Selecteur name
        context: Optional Airflow task context
        nom_projet: Project name
        local_dir: Local directory used to build local_path (default: /tmp)

    Returns:
        SelecteurStorageInfo object

    Raises:
        ConfigError: If no configuration is found
    """
    configs = _get_selecteur_storage_info(
        context=context, nom_projet=nom_projet, selecteur=selecteur, local_dir=local_dir
    )

    if not configs:
        raise ConfigError(
            message=f"No storage info found for project {nom_projet} and selecteur {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
        )

    return configs[0]


def get_source_grist() -> list[SelecteurStorageInfo]:
    """Get SelecteurStorageInfo for all selecteurs with grist source."""
    return _get_selecteur_storage_info(only_source=True, only_grist=True)


def get_source_fichier() -> list[SelecteurStorageInfo]:
    """Get SelecteurStorageInfo for all selecteurs with file source."""
    return _get_selecteur_storage_info(only_source=True, only_fichier=True)
