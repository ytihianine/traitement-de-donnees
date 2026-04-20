"""Factory for creating database handlers."""

from infra.database.base import DBInterface
from infra.database.postgres import PgAdapter
from infra.database.sqlite import SQLiteAdapter
from infra.database.trino import TrinoAdapter

from _enums.database import DatabaseType


def _check_required_params(db_type: DatabaseType, **db_config: dict) -> None:
    """Check if required parameters are present for the given database type."""
    if db_type == DatabaseType.TRINO:
        required_params = ["host", "user", "catalog", "port", "schema"]
        missing_params = [param for param in required_params if param not in db_config]
        if missing_params:
            raise ValueError(
                f"Missing required parameters for Trino: {', '.join(missing_params)}"
            )

    if db_type in [DatabaseType.POSTGRES, DatabaseType.SQLITE]:
        if "connection_id" not in db_config:
            raise ValueError(
                f"Missing required parameter 'connection_id' for {db_type.value}"
            )


def create_db_handler(
    db_type: DatabaseType = DatabaseType.POSTGRES, **db_config
) -> DBInterface:
    """Create a database handler based on connection type.

    Args:
        connection_id: Airflow connection ID or database path for sqlite
        db_type: Type of database ('postgres', etc.)

    Returns:
        A database handler instance

    Raises:
        ValueError: If db_type is not supported
    """

    _check_required_params(db_type, **db_config)

    if db_type == DatabaseType.POSTGRES:
        return PgAdapter(connection_id=db_config.get("connection_id", ""))
    elif db_type == DatabaseType.TRINO:
        return TrinoAdapter(
            host=db_config.get("host", ""),
            user=db_config.get("user", ""),
            catalog=db_config.get("catalog", ""),
            port=db_config.get("port", 443),
            schema=db_config.get("schema", None),
            http_scheme=db_config.get("http_scheme", "https"),
            verify=db_config.get("verify", True),
        )
    elif db_type == DatabaseType.SQLITE:
        return SQLiteAdapter(connection_id=db_config.get("connection_id", ""))

    raise ValueError(f"Unsupported database type: {db_type}")
