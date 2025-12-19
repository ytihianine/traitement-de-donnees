from typing import Any
from datetime import datetime
from psycopg2 import extensions, sql, errors


def drop_partitions(
    partitions: list[tuple[Any, ...]], cursor: extensions.cursor, dry_run: bool = True
) -> None:
    """
    Supprime toutes les partitions d'un schéma spécifique.

    Args:
        connection_params (dict): Paramètres de connexion PostgreSQL
        schema_name (str): Nom du schéma
        dry_run (bool): Si True, affiche les commandes sans les exécuter
    """
    print(f"{len(partitions)} partition(s) trouvée(s)")

    # Suppression des partitions
    dropped_count = 0
    for partition_name, parent_table, schema in partitions:
        drop_query = sql.SQL(string="DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(schema), sql.Identifier(partition_name)
        )

        if dry_run:
            print(f"[DRY RUN] {drop_query.as_string(context=cursor)}")
        else:
            cursor.execute(query=drop_query)
            print(f"✓ Supprimée: {schema}.{partition_name}")
            dropped_count += 1

    if not dry_run:
        print(
            f"\n{dropped_count}/{len(partitions)} partition(s) supprimée(s) avec succès"
        )
    else:
        print(f"\n[DRY RUN] {len(partitions)} partition(s) seraient supprimées")

    cursor.close()


def create_partitions(
    tbl_names: list[tuple[Any, ...]],
    range_start: datetime,
    range_end: datetime,
    cursor: extensions.cursor,
    dry_run: bool = True,
) -> None:
    print(f"{len(tbl_names)} table(s) trouvée(s)")

    created_count = 0
    for tbl_name, schema in tbl_names:
        # Nom de la partition : parenttable_YYYY_MM
        partition_name = "_".join(
            [
                tbl_name,
                range_start.strftime(format="%Y%m%d"),
                range_end.strftime(format="%Y%m%d"),
            ]
        )

        print(f"Creating partition {partition_name} for {tbl_name}.")
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{partition_name}
            PARTITION OF {schema}.{tbl_name}
            FOR VALUES FROM ('{range_start}') TO ('{range_end}');
        """

        if dry_run:
            print(f"[DRY RUN] {create_query}")
        else:
            cursor.execute(query=create_query)
            print(f"✓ Partition {partition_name} created successfully.")
            created_count += 1


def update_import_timestamp(
    schema: str,
    tbl_name: str,
    range_start: datetime,
    range_end: datetime,
    curseur: extensions.cursor,
) -> None:
    # Nom de la partition : parenttable_YYYY_MM
    partition_name = "_".join(
        [
            tbl_name,
            range_start.strftime(format="%Y%m%d"),
            range_end.strftime(format="%Y%m%d"),
        ]
    )

    try:
        print(f"Creating partition {partition_name} for {tbl_name}.")
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{partition_name}
            PARTITION OF {schema}.{tbl_name}
            FOR VALUES FROM ('{range_start}') TO ('{range_end}');
        """
        curseur.execute(query=create_sql)
        print(f"Partition {partition_name} created successfully.")
    except errors.DuplicateTable:
        print(f"Partition {partition_name} already exists. Skipping creation.")
    except Exception as e:
        print(f"Error creating partition {partition_name}: {str(e)}")
        raise
