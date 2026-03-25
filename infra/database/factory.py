"""Factory for creating database handlers."""

from typing import Optional

from infra.database.base import BaseDBHandler
from infra.database.postgres import PostgresDBHandler
from infra.database.sqlite import SQLiteDBHandler
from infra.database.trino import TrinoDBHandler

from enums.database import DatabaseType


def create_db_handler(
    connection_id: str, db_type: DatabaseType = DatabaseType.POSTGRES
) -> BaseDBHandler:
    """Create a database handler based on connection type.

    Args:
        connection_id: Airflow connection ID or database path for sqlite
        db_type: Type of database ('postgres', etc.)

    Returns:
        A database handler instance

    Raises:
        ValueError: If db_type is not supported
    """
    if db_type == DatabaseType.POSTGRES:
        return PostgresDBHandler(connection_id)
    elif db_type == DatabaseType.SQLITE:
        return SQLiteDBHandler(connection_id)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def create_trino_handler(
    host: str,
    user: str,
    catalog: str,
    port: int = 443,
    schema: Optional[str] = None,
    http_scheme: str = "https",
    verify: bool = True,
) -> TrinoDBHandler:
    """Create a read-only Trino database handler.

    Args:
        host: Trino coordinator host.
        user: User used for authentication.
        catalog: Default catalog to query.
        port: Trino coordinator port. Defaults to 443.
        schema: Default schema within the catalog.
        http_scheme: HTTP scheme ('http' or 'https'). Defaults to 'https'.
        verify: Whether to verify SSL certificates. Defaults to True.

    Returns:
        A TrinoDBHandler instance.
    """
    return TrinoDBHandler(
        host=host,
        user=user,
        catalog=catalog,
        port=port,
        schema=schema,
        http_scheme=http_scheme,
        verify=verify,
    )
