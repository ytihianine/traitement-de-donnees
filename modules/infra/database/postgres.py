"""PostgreSQL database handler implementation."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from modules.infra.database.base import DBInterface

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from sqlalchemy.engine.base import Engine


class PgAdapter(DBInterface):
    """Handler for PostgreSQL database operations.

    Supports two initialization modes:
    - Airflow mode: via a connection_id referencing an Airflow connection.
    - Local mode: via direct psycopg2 connection parameters (host, port, dbname, user, password).
    """

    def __init__(
        self,
        connection_id: str | None = None,
        *,
        host: str | None = None,
        port: int = 5432,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ):
        """Initialize with either an Airflow connection ID or local psycopg2 parameters.

        Args:
            connection_id: Airflow connection ID (Airflow mode).
            host: Database host (local mode).
            port: Database port (local mode, default 5432).
            dbname: Database name (local mode).
            user: Database user (local mode).
            password: Database password (local mode).
        """
        if connection_id and host:
            raise ValueError("Provide either connection_id (Airflow) or host/dbname/user/password (local), not both.")
        if not connection_id and not host:
            raise ValueError("Provide either connection_id (Airflow) or host/dbname/user/password (local).")

        self._use_airflow = connection_id is not None
        self.connection_id = connection_id
        self._local_params: dict[str, Any] = {}
        if not self._use_airflow:
            self._local_params = {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password,
            }

        self._hook: DbApiHook | None = None
        self._engine: Engine | None = None

    @property
    def hook(self) -> DbApiHook:
        """Lazy initialization of PostgresHook (Airflow mode only)."""
        if not self._use_airflow:
            raise RuntimeError("hook is not available in local mode. Use get_conn() instead.")
        if self._hook is None:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            self._hook = PostgresHook(postgres_conn_id=self.connection_id)
        return self._hook

    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine for DataFrame operations."""
        if self._engine is None:
            if self._use_airflow:
                self._engine = cast("Engine", self.hook.get_sqlalchemy_engine())
            else:
                p = self._local_params
                uri = f"postgresql://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['dbname']}"
                self._engine = create_engine(uri)
        return self._engine

    @property
    def connection(self) -> dict[str, Any]:
        if self._use_airflow:
            return self.hook.connection.to_dict()
        return self._local_params

    def get_uri(self) -> str:
        """Get the database URI."""
        if self._use_airflow:
            return self.hook.get_uri()
        p = self._local_params
        return f"postgresql://{p['user']}:****@{p['host']}:{p['port']}/{p['dbname']}"

    def get_conn(self) -> Any:
        """Get a database connection."""
        if self._use_airflow:
            return self.hook.get_conn()
        return psycopg2.connect(**self._local_params)

    def execute(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> None:
        """Execute a query without returning results."""
        start_time = time.time()
        if self._use_airflow:
            self.hook.run(query, parameters=parameters)
        else:
            with self.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, parameters)
                conn.commit()
        logging.debug(f"Query executed in {time.time() - start_time:.2f}s")

    def fetch_one(
        self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Fetch a single row as a dictionary."""
        start_time = time.time()
        with self.get_conn() as conn, conn.cursor() as cur:
            cur.execute(query, parameters)
            if cur.description is None:
                raise ValueError("Query did not return a result set")
            columns = [str(desc[0]) for desc in cur.description]
            result = cur.fetchone()

        logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
        if result is None:
            return None
        return dict(zip(columns, result, strict=False))

    def fetch_all(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Fetch all rows as a list of dictionaries."""
        start_time = time.time()
        with self.get_conn() as conn, conn.cursor() as cur:
            cur.execute(query, parameters)
            if cur.description is None:
                raise ValueError("Query did not return a result set")

            columns = [str(desc[0]) for desc in cur.description]
            results = cur.fetchall()

        logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
        return [dict(zip(columns, row, strict=False)) for row in results]

    def fetch_df(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> pd.DataFrame:
        """Fetch results as a pandas DataFrame."""
        start_time = time.time()
        logging.info(msg=f"Running statement:\n {query}")
        if self._use_airflow:
            df = self.hook.get_pandas_df(sql=query, parameters=parameters)
        else:
            df = pd.read_sql(sql=query, con=self.engine, params=parameters)
        logging.debug(msg=f"Query executed in {time.time() - start_time:.2f}s")
        return df

    def fetch_table_columns(self, schema: str, table: str) -> list[str]:
        """Fetch the column names of a table."""
        df = self.fetch_df(
            query="""
                SELECT isc.table_catalog, isc.table_schema, isc.table_name, isc.column_name
                FROM information_schema.columns isc
                WHERE
                    isc.table_schema = %s
                    AND isc.table_name = %s
                    AND isc.column_default IS NULL
                    AND isc.is_identity = 'NO'
                ORDER BY table_schema ASC, table_name ASC, column_name ASC;
            """,
            parameters=(schema, table),
        )
        return df.loc[:, "column_name"].tolist()

    def fetch_table_pk(self, schema: str, table: str) -> list[str]:
        """Fetch the primary key column of a table."""
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                    AND tc.constraint_schema = kcu.constraint_schema
            WHERE tc.table_schema = %s
                AND tc.table_name = %s
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
        """
        df = self.fetch_df(query, parameters=(schema, table))
        return df.loc[:, "column_name"].tolist()

    def insert(self, table: str, data: dict[str, Any]) -> None:
        """Insert a single row into a table."""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ["%s"] * len(columns)

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        self.execute(query, tuple(values))

    def bulk_insert(self, table: str, data: list[dict[str, Any]]) -> None:
        """Insert multiple rows into a table."""
        if not data:
            return

        columns = list(data[0].keys())
        values = [[row[col] for col in columns] for row in data]
        placeholders = ["%s"] * len(columns)

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        with self.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, values)
            conn.commit()

    def update(self, table: str, data: dict[str, Any], where: dict[str, Any]) -> None:
        """Update rows in a table."""
        set_clause = ", ".join([f"{k} = %s" for k in data])
        where_clause = " AND ".join([f"{k} = %s" for k in where])

        query = f"""
            UPDATE {table}
            SET {set_clause}
            WHERE {where_clause}
        """

        parameters = tuple(list(data.values()) + list(where.values()))
        self.execute(query, parameters)

    def delete(self, table: str, where: dict[str, Any]) -> None:
        """Delete rows from a table."""
        where_clause = " AND ".join([f"{k} = %s" for k in where])

        query = f"""
            DELETE FROM {table}
            WHERE {where_clause}
        """

        self.execute(query, tuple(where.values()))

    def begin(self) -> None:
        """Begin a transaction."""
        self.execute("BEGIN")

    def commit(self) -> None:
        """Commit the current transaction."""
        self.execute("COMMIT")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.execute("ROLLBACK")

    def copy_expert(self, sql: str, filepath: str) -> None:
        """
        Execute PostgreSQL COPY command using file-like object for efficient bulk data transfer.

        Args:
            sql: COPY command to execute
            filepath: Path to the file to bulk load
        """
        start_time = time.time()
        if self._use_airflow:
            self.hook.copy_expert(sql, filepath)
        else:
            with self.get_conn() as conn:
                with conn.cursor() as cur, open(filepath) as f:
                    cur.copy_expert(sql, f)
                conn.commit()
        logging.debug(f"COPY operation executed in {time.time() - start_time:.2f}s")
