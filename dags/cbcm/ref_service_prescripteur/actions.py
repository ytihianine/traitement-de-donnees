from airflow.sdk import Variable
from infra.http_client.adapters import RequestsClient
from infra.http_client.config import ClientConfig
import pandas as pd

from infra.grist.client import GristAPI
from infra.database.factory import create_db_handler
from utils.config.vars import AGENT, DEFAULT_GRIST_HOST, DEFAULT_PG_DATA_CONN_ID, PROXY


def get_all_cf_cc() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(
        query="""
            select
                distinct
                centre_financier,
                centre_cout
            from
                donnee_comptable.demande_achat
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            union
            select
                distinct
                centre_financier,
                centre_cout
            from
                donnee_comptable.engagement_juridique
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            union
            select
                distinct
                centre_financier,
                centre_cout
            from
                donnee_comptable.delai_global_paiement
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            union
            select
                distinct
                centre_financier,
                centre_cout
            from
                donnee_comptable.demande_paiement_complet
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            ;
        """
    )

    return df


def get_demande_achat() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(
        query="""
            SELECT
                id_da,
                centre_financier,
                centre_cout
            FROM donnee_comptable.demande_achat
            GROUP BY
                id_da, centre_financier, centre_cout
            ;
        """
    )

    return df


def get_demande_paiement_complet() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(
        query="""
            select
                id_dp,
                centre_financier,
                centre_cout,
                unique_multi
            from
                donnee_comptable.demande_paiement_complet
            group by
                id_dp,
                centre_financier,
                centre_cout,
                unique_multi
            ;
        """
    )

    return df


def get_delai_global_paiement() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(
        query="""
            select
                id_row_dgp::text,
                centre_financier,
                centre_cout
            from
                donnee_comptable.delai_global_paiement
            group by
                id_row_dgp,
                centre_financier,
                centre_cout
            ;
        """
    )

    return df


def get_engagement_juridique() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(
        query="""
            select
                id_ej,
                centre_financier,
                centre_cout
            from
                donnee_comptable.engagement_juridique
            group by
                id_ej,
                centre_financier,
                centre_cout
            ;
        """
    )

    return df
