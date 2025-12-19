from enum import Enum, auto
from psycopg2 import extensions


class Actions(Enum):
    DROP = auto()
    CREATE = auto()


def get_partitions(schema: str, curseur: extensions.cursor) -> list[tuple[str, ...]]:
    curseur.execute(
        query=f"""
        SELECT
            child.relname AS partition_name,
            parent.relname AS parent_table,
            child.relnamespace::regnamespace::text AS schema_name
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace nmsp_parent ON parent.relnamespace = nmsp_parent.oid
        WHERE nmsp_parent.nspname = '{schema}'
        AND child.relispartition = true
        ORDER BY parent.relname, child.relname;
        """
    )
    return curseur.fetchall()
