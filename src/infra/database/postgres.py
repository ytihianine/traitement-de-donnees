"""PostgreSQL database handler implementation."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple, cast

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from .base import DBInterface
from .exceptions import DatabaseError


class PgAdapter(DBInterface):
    """Handler for PostgreSQL database operations.

    Supports two initialization modes:
    - Airflow mode: via a connection_id referencing an Airflow connection.
    - Local mode: via direct psycopg2 connection parameters (host, port, dbname, user, password).
    """

    def __init__(
        self,
        connection_id: Optional[str] = None,
        *,
        host: Optional[str] = None,
        port: int = 5432,
        dbname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
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
            raise ValueError(
                "Provide either connection_id (Airflow) or host/dbname/user/password (local), not both."
            )
        if not connection_id and not host:
            raise ValueError(
                "Provide either connection_id (Airflow) or host/dbname/user/password (local)."
            )

        self._use_airflow = connection_id is not None
        self.connection_id = connection_id
        self._local_params: Dict[str, Any] = {}
        if not self._use_airflow:
            self._local_params = {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "password": password,
            }

        self._hook: Optional[Any] = None
        self._engine: Optional[Engine] = None

    @property
    def hook(self) -> Any:
        """Lazy initialization of PostgresHook (Airflow mode only)."""
        if not self._use_airflow:
            raise RuntimeError(
                "hook is not available in local mode. Use get_conn() instead."
            )
        if self._hook is None:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            self._hook = PostgresHook(postgres_conn_id=self.connection_id)
        return self._hook

    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine for DataFrame operations."""
        if self._engine is None:
            if self._use_airflow:
                self._engine = cast(Engine, self.hook.get_sqlalchemy_engine())
            else:
                p = self._local_params
                uri = f"postgresql://{p['user']}:{p['password']}@{p['host']}:{p['port']}/{p['dbname']}"
                self._engine = create_engine(uri)
        return self._engine

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

    def execute(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> None:
        """Execute a query without returning results."""
        try:
            start_time = time.time()
            if self._use_airflow:
                self.hook.run(query, parameters=parameters)
            else:
                with self.get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(query, parameters)
                    conn.commit()
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
        except Exception as e:
            raise DatabaseError(f"Error executing query: {str(e)}") from e

    def fetch_one(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single row as a dictionary."""
        try:
            start_time = time.time()
            with self.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, parameters)
                    if cur.description is None:
                        raise DatabaseError("Query did not return a result set")
                    columns = [str(desc[0]) for desc in cur.description]
                    result = cur.fetchone()

            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
            if result is None:
                return None
            return dict(zip(columns, result))
        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(f"Error fetching row: {str(e)}") from e

    def fetch_all(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all rows as a list of dictionaries."""
        try:
            start_time = time.time()
            with self.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, parameters)
                    if cur.description is None:
                        raise DatabaseError("Query did not return a result set")

                    columns = [str(desc[0]) for desc in cur.description]
                    results = cur.fetchall()

            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
            return [dict(zip(columns, row)) for row in results]

        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(f"Error fetching rows: {str(e)}") from e

    def fetch_df(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Fetch results as a pandas DataFrame."""
        try:
            start_time = time.time()
            logging.info(msg=f"Running statement:\n {query}")
            if self._use_airflow:
                df = self.hook.get_pandas_df(sql=query, parameters=parameters)
            else:
                df = pd.read_sql(sql=query, con=self.engine, params=parameters)
            logging.debug(msg=f"Query executed in {time.time() - start_time:.2f}s")
            return df
        except Exception as e:
            raise DatabaseError(f"Error fetching DataFrame: {str(e)}") from e

    def insert(self, table: str, data: Dict[str, Any]) -> None:
        """Insert a single row into a table."""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ["%s"] * len(columns)

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        self.execute(query, tuple(values))

    def bulk_insert(self, table: str, data: List[Dict[str, Any]]) -> None:
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

    def update(self, table: str, data: Dict[str, Any], where: Dict[str, Any]) -> None:
        """Update rows in a table."""
        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = %s" for k in where.keys()])

        query = f"""
            UPDATE {table}
            SET {set_clause}
            WHERE {where_clause}
        """

        parameters = tuple(list(data.values()) + list(where.values()))
        self.execute(query, parameters)

    def delete(self, table: str, where: Dict[str, Any]) -> None:
        """Delete rows from a table."""
        where_clause = " AND ".join([f"{k} = %s" for k in where.keys()])

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
        try:
            start_time = time.time()
            if self._use_airflow:
                self.hook.copy_expert(sql, filepath)
            else:
                with self.get_conn() as conn:
                    with conn.cursor() as cur:
                        with open(filepath, "r") as f:
                            cur.copy_expert(sql, f)
                    conn.commit()
            logging.debug(f"COPY operation executed in {time.time() - start_time:.2f}s")
        except Exception as e:
            raise DatabaseError(f"Error during COPY operation: {str(e)}") from e
