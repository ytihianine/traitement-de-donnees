import logging
from airflow.sdk import Variable
from infra.grist.client import GristAPI
from infra.http_client.adapters import RequestsClient
from infra.http_client.config import ClientConfig
import pandas as pd

from infra.database.factory import create_db_handler

from utils.config.vars import AGENT, DEFAULT_GRIST_HOST, DEFAULT_PG_DATA_CONN_ID, PROXY


def pg_info_scan() -> pd.DataFrame:
    # Hook
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Get postgres catalog
    df = db_handler.fetch_df(
        query="""
            SELECT
                t.table_schema,
                t.table_name,
                c.column_name,
                c.data_type
            FROM information_schema.tables t
            JOIN information_schema.columns c
                ON t.table_schema = c.table_schema
                AND t.table_name = c.table_name
            JOIN pg_class cls
                ON cls.relname = t.table_name
            JOIN pg_namespace ns
                ON ns.oid = cls.relnamespace
                AND ns.nspname = t.table_schema
            WHERE t.table_type = 'BASE TABLE'
                AND ns.nspname NOT IN ('pg_catalog', 'information_schema', 'documentation', 'temporaire')
                AND ns.nspname NOT LIKE '%_file_upload'
                AND cls.relispartition = FALSE      -- EXCLUDE PARTITION CHILD TABLES
            ORDER BY t.table_schema, t.table_name, c.ordinal_position;
        """
    )

    return df


def get_catalogue() -> pd.DataFrame:
    # Hook
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Get postgres catalog
    df = db_handler.fetch_df(
        query="""
            SELECT
                doccat.schema_name,
                doccat.table_name
            FROM documentation."catalogue" doccat;
        """
    )

    return df


def get_dictionnaire() -> pd.DataFrame:
    # Hook
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Get postgres catalog
    df = db_handler.fetch_df(
        query="""
            SELECT
                doccat.schema_name,
                doccat.table_name,
                docdic.variable
            FROM documentation."catalogue" doccat
            LEFT JOIN documentation."dictionnaire" docdic
            ON doccat.id = docdic.id_catalogue
            ;
        """
    )

    return df


def load_catalogue(df: pd.DataFrame) -> None:
    # Intégrer ces lignes dans Grist
    new_rows = df.to_dict(orient="records")
    logging.info(msg=f"Nombre de nouvelles lignes: {len(new_rows)}")

    if len(new_rows) > 0:
        logging.info(msg="Ajout des nouvelles lignes dans Grist")
        data = {"records": [{"fields": record} for record in new_rows]}

        logging.info(msg=f"Exemple: {data['records'][0]}")

        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
        request_client = RequestsClient(config=http_config)
        grist_client = GristAPI(
            http_client=request_client,
            base_url=DEFAULT_GRIST_HOST,
            workspace_id="catalogue",
            doc_id=Variable.get(key="grist_doc_id_catalogue"),
            api_token=Variable.get(key="grist_secret_key"),
        )

        grist_client.post_records(tbl_name="Catalogue", json=data)
    else:
        logging.info(msg="Aucune ligne à ajouter dans le catalogue ...")


def load_dictionnaire(df: pd.DataFrame) -> None:
    # Intégrer ces lignes dans Grist
    new_rows = df.to_dict(orient="records")
    logging.info(msg=f"Nombre de nouvelles lignes: {len(new_rows)}")

    if len(new_rows) > 0:
        logging.info(msg="Ajout des nouvelles lignes dans Grist")
        data = {"records": [{"fields": record} for record in new_rows]}

        logging.info(msg=f"Exemple: {data['records'][0]}")

        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
        request_client = RequestsClient(config=http_config)
        grist_client = GristAPI(
            http_client=request_client,
            base_url=DEFAULT_GRIST_HOST,
            workspace_id="catalogue",
            doc_id=Variable.get(key="grist_doc_id_catalogue"),
            api_token=Variable.get(key="grist_secret_key"),
        )

        grist_client.post_records(tbl_name="Dictionnaire", json=data)
    else:
        logging.info(msg="Aucune ligne à ajouter dans le dictionnaire ...")
