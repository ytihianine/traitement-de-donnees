import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np

import psycopg2
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import execute_values

from utils.control.structures import normalize_grist_dataframe
from utils.dataframe import df_info
from scripts.settings import get_settings

from dags.applications.configuration_projets import process

register_adapter(typ=np.int64, callable=AsIs)


# Tbl order is important due to tbl dependancy !
tbl_ordered = [
    {"tbl_name": "ref_direction", "process_func": process.process_direction},
    {"tbl_name": "ref_service", "process_func": process.process_service},
    {"tbl_name": "projet", "process_func": process.process_projet},
    {"tbl_name": "projet_contact", "process_func": process.process_projet_contact},
    {
        "tbl_name": "projet_documentation",
        "process_func": process.process_projet_documentation,
    },
    {"tbl_name": "projet_s3", "process_func": process.process_projet_s3},
    {"tbl_name": "projet_selecteur", "process_func": process.process_selecteur},
    {"tbl_name": "selecteur_source", "process_func": process.process_source},
    {
        "tbl_name": "selecteur_column_mapping",
        "process_func": process.process_col_mapping,
    },
    {"tbl_name": "selecteur_s3", "process_func": process.process_selecteur_s3},
    {
        "tbl_name": "selecteur_database",
        "process_func": process.process_selecteur_database,
    },
]
tbl_names = [action["tbl_name"] for action in tbl_ordered]
print(tbl_names)
tbl_names.reverse()

settings = get_settings()
DRY_RUN = False
NOW = datetime.now()
ADD_METADATA = True
schema = settings.load_config.schema
db_path = settings.load_config.grist_db_path

sqlite_conn = sqlite3.connect(db_path)
pg_conn = psycopg2.connect(
    host=settings.db.host,
    port=settings.db.port,
    dbname=settings.db.name,
    user=settings.db.user,
    password=settings.db.password,
)
pg_cur = pg_conn.cursor()
# for each tbl

if not DRY_RUN:
    for tbl_desc in tbl_names:
        # Delete rows
        drop_query = f"DELETE FROM {schema}.{tbl_desc};"
        print(drop_query)
        pg_cur.execute(query=drop_query)

for tbl in tbl_ordered:
    print("\n", "=" * 50)
    print(f"Start processing table <{tbl["tbl_name"]}>")
    # read data for sqlite file
    df = pd.read_sql_query(
        sql=f"SELECT * FROM {tbl["tbl_name"].capitalize()}", con=sqlite_conn
    )
    df = normalize_grist_dataframe(df=df)
    df_info(df=df, df_name=tbl["tbl_name"].capitalize())
    # apply process function
    df = tbl["process_func"](df=df)
    # df = df.drop(df.filter(regex="^(grist|manual)").columns, axis=1)
    df = df.fillna(np.nan).replace([np.nan], [None])

    # Add metadata
    if ADD_METADATA:
        df["import_timestamp"] = NOW
        df["import_date"] = NOW.date()
        df["snapshot_id"] = NOW.strftime(format="%Y%m%d_%H:%M:%S")

    # Show results
    # df = df.convert_dtypes()
    print(df.columns)
    print(df.dtypes)
    print(df.isnull().sum())
    df_info(df=df, df_name=tbl["tbl_name"].capitalize())

    if not DRY_RUN:
        # Get tbl columns and order them
        fetch_query = f"SELECT * FROM {schema}.{tbl["tbl_name"]} LIMIT 0;"
        pg_cur.execute(query=fetch_query)
        if pg_cur.description:
            sorted_cols = sorted(
                [col.name for col in pg_cur.description if col.name in df.columns]
            )
            print(sorted_cols)

            # load data to config db
            insert_records = df[sorted_cols].to_records(index=False).tolist()
            insert_query = f"INSERT INTO {schema}.{tbl["tbl_name"]} ({", ".join(sorted_cols)}) VALUES %s"  # noqa
            print(insert_query)
            execute_values(cur=pg_cur, sql=insert_query, argslist=insert_records)
            print(f"End processing table <{tbl["tbl_name"]}>\n")
        else:
            print("No results retrieved ...")

pg_conn.commit()
pg_conn.close()
sqlite_conn.close()
