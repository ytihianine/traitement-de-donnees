import os
import pandas as pd
import psycopg2

from utils.config.vars import custom_logger
from utils.control.structures import are_lists_egal

# FIXED VARIABLES
TSV_FILE_PATH = "/tmp/df_result.tsv"
ENV = os.environ.copy()

# USER VARIABLES
FILE_PATH = ""
DB_ID_COLNAME = "id"
SCHEMA = ""
TABLE_NAME = ""
IMPORT_TIMESTAMP = "2025-11-01 12:00:00.000"
SNAPSHOT_ID = "20251101_12:00:00"

pg_conn = psycopg2.connect(
    host=ENV["CONFIG_DB_HOST"],
    port=ENV["CONFIG_DB_PORT"],
    dbname=ENV["CONFIG_DB_NAME"],
    user=ENV["CONFIG_DB_USER"],
    password=ENV["CONFIG_DB_PASSWORD"],
)
pg_cur = pg_conn.cursor()

# get table columns sorted and remove ID columns
pg_cur.execute(
    query=f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{SCHEMA}'
    AND table_name = '{TABLE_NAME}'
    AND column_name != '{DB_ID_COLNAME}'
    ORDER BY column_name
"""
)
db_columns = [row[0] for row in pg_cur.fetchall()]
custom_logger.info(msg=f"Database columns sorted: {db_columns}")

# read data
df = pd.read_parquet(path=FILE_PATH)

# Add metadata columns
df["import_timestamp"] = pd.to_datetime(IMPORT_TIMESTAMP)
df["import_date"] = pd.to_datetime(IMPORT_TIMESTAMP)
df["snapshot_id"] = SNAPSHOT_ID

# Drop ID columns and sort colnames
df = df.drop(columns=[DB_ID_COLNAME], errors="ignore")
df_columns = sorted(df.columns)
df = df[df_columns]
custom_logger.info(msg=f"Dataframe columns sorted: {df_columns}")

# export file to csv using "\" as sep
df.to_csv(path_or_buf=TSV_FILE_PATH, sep="\t", index=False)

# Copy expert to db
if not are_lists_egal(list_A=list(df.columns), list_B=db_columns):
    raise ValueError("Erreur")

with open(file=TSV_FILE_PATH, mode="r") as f:
    pg_cur.copy_expert(
        sql=f"""
            COPY {SCHEMA}.{TABLE_NAME} ({', '.join(db_columns)})
            FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER E'\t',
                HEADER TRUE,
                NULL 'NULL'
            );
        """,
        file=f,
    )

pg_conn.commit()
pg_conn.close()

# delete tmp file
os.remove(path=TSV_FILE_PATH)
