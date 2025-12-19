import os
from datetime import datetime, timedelta

import psycopg2

from scripts.handle_partitions.commun import get_tbl_names, get_partitions, Actions
from scripts.handle_partitions.partitions import (
    drop_partitions,
    create_partitions,
    update_import_timestamp,
    update_snapshot_id,
)

# VARIABLES
ENV = os.environ.copy()
schema = "siep"
tbl_to_excludes = ("conso", "bien_info", "ref", "bien_georisque", "facture")
action = Actions.UPDATE_SNAPSHOT

# Si Actions.CREATE
str_date = "31/07/2025 00:00:00"
from_date = datetime.strptime(str_date, "%d/%m/%Y %H:%M:%S")
to_date = from_date + timedelta(days=1)
print(from_date, to_date)

# Si Actions.UPDATE_TIMESTAMP
curr_import_timestamp = datetime.strptime("2025-12-19 15:15:00", "%Y-%m-%d %H:%M:%S")
new_import_timestamp = datetime.strptime("2025-07-31 00:00:00", "%Y-%m-%d %H:%M:%S")
print(curr_import_timestamp, new_import_timestamp)

# Si Actions.UPDATE_SNAPSHOT
current_snapshot_id = "20250831_12:00:00"
new_snapshot_id = "20250731_12:00:00"
print(current_snapshot_id, new_snapshot_id)


if __name__ == "__main__":
    # Connect to database
    pg_conn = psycopg2.connect(
        host=ENV["CONFIG_DB_HOST"],
        port=ENV["CONFIG_DB_PORT"],
        dbname=ENV["CONFIG_DB_NAME"],
        user=ENV["CONFIG_DB_USER"],
        password=ENV["CONFIG_DB_PASSWORD"],
    )
    pg_cur = pg_conn.cursor()

    # Action to perform
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

    if action == Actions.UPDATE_TIMESTAMP:
        try:
            # Récupérer la liste des tables
            table_names = get_tbl_names(schema=schema, curseur=pg_cur)
            table_names = [
                tbl for tbl in table_names if not tbl[0].startswith(tbl_to_excludes)
            ]

            update_import_timestamp(
                tbl_names=table_names,
                current_import_timestamp=curr_import_timestamp,
                new_import_timestamp=new_import_timestamp,
                cursor=pg_cur,
                dry_run=True,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            print(
                f"✗ Erreur lors de la création de partitions dans le schéma {schema}: {e}"
            )

    if action == Actions.UPDATE_SNAPSHOT:
        try:
            # Récupérer la liste des tables
            table_names = get_tbl_names(schema=schema, curseur=pg_cur)
            table_names = [
                tbl for tbl in table_names if not tbl[0].startswith(tbl_to_excludes)
            ]

            update_snapshot_id(
                tbl_names=table_names,
                current_snapshot_id=current_snapshot_id,
                new_snapshot_id=new_snapshot_id,
                cursor=pg_cur,
                dry_run=True,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            print(
                f"✗ Erreur lors de la création de partitions dans le schéma {schema}: {e}"
            )
