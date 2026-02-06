from typing import Any, Mapping
import pandas as pd
from airflow.sdk import Variable

from infra.http_client.adapters import RequestsClient
from infra.http_client.config import ClientConfig

from infra.grist.client import GristAPI
from utils.config.vars import AGENT, DEFAULT_GRIST_HOST, DEFAULT_PG_DATA_CONN_ID, PROXY

from infra.database.factory import create_db_handler
from utils.config.dag_params import get_db_info, get_project_name
from utils.config.tasks import get_source_grist


def get_agent_db(context: Mapping[str, Any]) -> pd.DataFrame:
    schema = get_db_info(context=context).prod_schema

    # Hook
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Retrieve data
    df = db_handler.fetch_df(
        query=f"""
            select
                cra.matricule_agent,
                cra.nom_usuel,
                cra.prenom,
                cra.genre,
                cra.age,
                cracr.qualite_statutaire,
                cracr.dge_perimetre
            from
                {schema}.agent cra
            inner join {schema}.agent_carriere cracr
            on cra.matricule_agent = cracr.matricule_agent
            ;
        """
    )

    return df


def load_agent(
    df_get_agent_db: pd.DataFrame,
    df_agent: pd.DataFrame,
    grist_doc_selecteur: str,
    context: Mapping[str, Any],
) -> None:
    # Get Grist doc_id
    nom_projet = get_project_name(context=context)
    grist_doc_info = get_source_grist(
        nom_projet=nom_projet, selecteur=grist_doc_selecteur
    )

    # Merge pour comparer
    df = pd.merge(
        left=df_get_agent_db,
        right=df_agent["matricule_agent"],
        how="left",
        on=["matricule_agent"],
        indicator=True,
    )

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]
    df = df.drop(columns=["_merge", "genre", "age"])

    # Intégrer ces lignes dans Grist
    print(df.columns)
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristAPI(
        http_client=request_client,
        base_url=DEFAULT_GRIST_HOST,
        workspace_id="dsci",
        doc_id=grist_doc_info.id_source,
        api_token=Variable.get(key="grist_secret_key"),
    )
    grist_client.send_dataframe_to_grist(
        df=df,
        tbl_name="Agent",
        rename_columns={
            "matricule_agent": "Matricule_agent",
            "nom_usuel": "Nom_usuel",
            "prenom": "Prenom",
            "qualite_statutaire": "Qualite_statutaire",
        },
    )
