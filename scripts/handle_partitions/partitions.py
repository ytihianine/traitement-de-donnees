from typing import Any, Optional
from datetime import datetime
from psycopg2 import extensions, sql


def drop_partitions(
    partitions: list[tuple[Any, ...]],
    cursor: extensions.cursor,
    dry_run: bool = True,
    from_date: Optional[datetime] = None,
) -> None:
    """
    Supprime toutes les partitions d'un schéma spécifique.

    Args:
        connection_params (dict): Paramètres de connexion PostgreSQL
        schema_name (str): Nom du schéma
        dry_run (bool): Si True, affiche les commandes sans les exécuter
    """
    print(f"{len(partitions)} partition(s) trouvée(s)")

    if from_date is not None:
        partitions = [
            partition
            for partition in partitions
            if from_date.strftime(format="%Y%m%d") in partition
        ]

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

    if not dry_run:
        print(f"\n{created_count}/{len(tbl_names)} partitions(s) créé(s) avec succès")
    else:
        print(f"\n[DRY RUN] {len(tbl_names)} partitions(s) seraient créées")


def update_import_timestamp(
    tbl_names: list[tuple[Any, ...]],
    current_import_timestamp: datetime,
    new_import_timestamp: datetime,
    cursor: extensions.cursor,
    dry_run: bool = True,
) -> None:
    print(f"{len(tbl_names)} table(s) trouvée(s)")

    updated_count = 0
    for tbl_name, schema in tbl_names:
        print(f"Updating table {tbl_name}.")
        create_query = f"""
            UPDATE {schema}.{tbl_name}
            SET import_timestamp = '{new_import_timestamp}',
                import_date = '{new_import_timestamp.strftime(format="%Y-%m-%d")}'
            WHERE import_timestamp = '{current_import_timestamp}';
        """

        if dry_run:
            print(f"[DRY RUN] {create_query}")
        else:
            cursor.execute(query=create_query)
            print(f"✓ Table {tbl_name} updated successfully.")
            updated_count += 1

    if not dry_run:
        print(f"\n{updated_count}/{len(tbl_names)} table(s) mise(s) à jour avec succès")
    else:
        print(f"\n[DRY RUN] {len(tbl_names)} table(s) seraient misees à jour")


def update_snapshot_id(
    tbl_names: list[tuple[Any, ...]],
    current_snapshot_id: str,
    new_snapshot_id: str,
    cursor: extensions.cursor,
    dry_run: bool = True,
) -> None:
    print(f"{len(tbl_names)} table(s) trouvée(s)")

    updated_count = 0
    for tbl_name, schema in tbl_names:
        print(f"Updating table {tbl_name}.")
        create_query = f"""
            UPDATE {schema}.{tbl_name}
            SET snapshot_id = '{new_snapshot_id}'
            WHERE snapshot_id = '{current_snapshot_id}';
        """

        if dry_run:
            print(f"[DRY RUN] {create_query}")
        else:
            cursor.execute(query=create_query)
            print(f"✓ Table {tbl_name} updated successfully.")
            updated_count += 1

    if not dry_run:
        print(f"\n{updated_count}/{len(tbl_names)} table(s) mise(s) à jour avec succès")
    else:
        print(f"\n[DRY RUN] {len(tbl_names)} table(s) seraient misees à jour")
