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
                id_dgp::text,
                centre_financier,
                centre_cout
            from
                donnee_comptable.delai_global_paiement
            group by
                id_dgp,
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


def load_new_cf_cc(df_get_all_cf_cc: pd.DataFrame, df_sp: pd.DataFrame) -> None:
    # Filtrer les lignes
    df_get_all_cf_cc = df_get_all_cf_cc.loc[
        (df_get_all_cf_cc["centre_financier"] == "Ind")
        | (df_get_all_cf_cc["centre_cout"] == "Ind")
    ]

    # Merge pour comparer
    on_cols = ["centre_financier", "centre_cout"]
    df = pd.merge(
        left=df_get_all_cf_cc,
        right=df_sp[on_cols],
        how="left",
        on=on_cols,
        indicator=True,
    )

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only", on_cols]

    # Intégrer ces lignes dans Grist
    new_cf_cc = df.rename(
        columns={
            "centre_cout": "Centre_de_cout",
            "centre_financier": "Centre_financier",
        }
    ).to_dict(orient="records")
    print(f"Nouveau couple CF-CC sans SP: {len(new_cf_cc)}")

    if len(new_cf_cc) > 0:
        print("Ajout des nouveaux couples CF-CC dans Grist")
        data = {"records": [{"fields": record} for record in new_cf_cc]}

        print(f"Exemple: {data['records'][0]}")

        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
        request_client = RequestsClient(config=http_config)
        grist_client = GristAPI(
            http_client=request_client,
            base_url=DEFAULT_GRIST_HOST,
            workspace_id="dsci",
            doc_id=Variable.get(key="grist_doc_id_cbcm"),
            api_token=Variable.get(key="grist_secret_key"),
        )
        try:
            grist_client.post_records(tbl_name="Service_prescripteur", json=data)
        except Exception as e:
            print(f"Erreur lors de l'ajout des lignes: {e}")
    else:
        print("Aucun nouveau couple CF-CC ... Skipping")


def load_demande_achat(
    df_get_demande_achat: pd.DataFrame, df_demande_achat_sp_manuel: pd.DataFrame
) -> None:
    # Filtrer les lignes
    df_get_demande_achat = df_get_demande_achat.loc[
        (df_get_demande_achat["centre_financier"] == "Ind")
        | (df_get_demande_achat["centre_cout"] == "Ind")
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_demande_achat,
        right=df_demande_achat_sp_manuel,
        how="left",
        on=["id_da"],
        indicator=True,
    )

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]

    # Les envoyers dans Grist
    print(f"Nb de lignes sans cf_cc: {len(df)}")
    print(df.columns)


def load_demande_paiement_complet(
    df_get_demande_paiement_complet: pd.DataFrame,
    df_demande_paiement_sp_manuel: pd.DataFrame,
) -> None:
    # Filtrer les lignes
    df_get_demande_paiement_complet = df_get_demande_paiement_complet.loc[
        (df_get_demande_paiement_complet["centre_financier"] == "Ind")
        | (df_get_demande_paiement_complet["centre_cout"] == "Ind")
        | (df_get_demande_paiement_complet["unique_multi"] == "Multiple")
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_demande_paiement_complet,
        right=df_demande_paiement_sp_manuel,
        how="left",
        on=["id_dp"],
        indicator=True,
    )

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]

    # Intégrer ces lignes dans Grist
    print(f"Nb de lignes sans cf_cc: {len(df)}")
    print(df.columns)


def load_delai_global_paiement(
    df_get_delai_global_paiement: pd.DataFrame,
    df_delai_global_paiement_sp_manuel: pd.DataFrame,
) -> None:
    # Filtrer les lignes
    df_get_delai_global_paiement = df_get_delai_global_paiement.loc[
        (df_get_delai_global_paiement["centre_financier"] == "Ind")
        | (df_get_delai_global_paiement["centre_cout"] == "Ind")
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_delai_global_paiement,
        right=df_delai_global_paiement_sp_manuel,
        how="left",
        on=["id_dgp"],
        indicator=True,
    )

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]

    # Intégrer ces lignes dans Grist
    print(f"Nb de lignes sans cf_cc: {len(df)}")
    print(df.columns)


def load_engagement_juridique(
    df_get_engagement_juridique: pd.DataFrame,
    df_engagement_juridique_sp_manuel: pd.DataFrame,
) -> None:
    # Filtrer les lignes
    df_get_engagement_juridique = df_get_engagement_juridique.loc[
        (df_get_engagement_juridique["centre_financier"] == "Ind")
        | (df_get_engagement_juridique["centre_cout"] == "Ind")
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_engagement_juridique,
        right=df_engagement_juridique_sp_manuel,
        how="left",
        on=["id_ej"],
        indicator=True,
    )

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]

    # Intégrer ces lignes dans Grist
    print(f"Nb de lignes sans cf_cc: {len(df)}")
    print(df.columns)
