import os
from datetime import datetime, timedelta

import psycopg2

from scripts.handle_partitions.commun import get_tbl_names, get_partitions, Actions
from scripts.handle_partitions.partitions import drop_partitions, create_partitions

ENV = os.environ.copy()
pg_conn = psycopg2.connect(
    host=ENV["CONFIG_DB_HOST"],
    port=ENV["CONFIG_DB_PORT"],
    dbname=ENV["CONFIG_DB_NAME"],
    user=ENV["CONFIG_DB_USER"],
    password=ENV["CONFIG_DB_PASSWORD"],
)
pg_cur = pg_conn.cursor()


if __name__ == "__main__":
    # User variables
    schema = "siep"
    tbl_to_excludes = ("conso", "bien_info", "ref")
    action = Actions.CREATE

    # Si Actions.CREATE
    str_date = "31/07/2025 00:00:00"
    from_date = datetime.strptime(str_date, "%d/%m/%Y %H:%M:%S")
    to_date = from_date + timedelta(days=1)
    print(from_date, to_date)

    if action == Actions.DROP:
        try:
            # Récupérer la liste des partitions
            partition_names = get_partitions(schema=schema, curseur=pg_cur)
            partition_names = [
                partition
                for partition in partition_names
                if not partition[1].startswith(tbl_to_excludes)
            ]
            drop_partitions(partitions=partition_names, cursor=pg_cur, dry_run=True)
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            print(
                f"✗ Erreur lors de la suppression de partitions dans le schéma {schema}: {e}"
            )

    if action == Actions.CREATE:
        try:
            # Récupérer la liste des tables
            table_names = get_tbl_names(schema=schema, curseur=pg_cur)
            table_names = [
                tbl for tbl in table_names if not tbl[0].startswith(tbl_to_excludes)
            ]

            create_partitions(
                tbl_names=table_names,
                range_start=from_date,
                range_end=to_date,
                cursor=pg_cur,
                dry_run=True,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            print(
                f"✗ Erreur lors de la création de partitions dans le schéma {schema}: {e}"
            )
