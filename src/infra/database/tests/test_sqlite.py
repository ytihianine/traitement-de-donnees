"""Tests for SQLiteAdapter."""

import pytest

from src.infra.database.exceptions import DatabaseError
from src.infra.database.sqlite import SQLiteAdapter


@pytest.fixture()
def db() -> SQLiteAdapter:
    """Create an in-memory SQLite adapter for testing."""
    handler = SQLiteAdapter(connection_id=":memory:")
    handler.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
    )
    return handler


class TestSQLiteAdapterGetUri:
    def test_get_uri(self, db: SQLiteAdapter) -> None:
        assert db.get_uri() == "sqlite:///:memory:"


class TestSQLiteAdapterGetConn:
    def test_get_conn_returns_connection(self, db: SQLiteAdapter) -> None:
        conn = db.get_conn()
        assert conn is not None


class TestSQLiteAdapterExecute:
    def test_execute_creates_table(self) -> None:
        handler = SQLiteAdapter(connection_id=":memory:")
        handler.execute("CREATE TABLE t (id INTEGER)")
        result = handler.fetch_all(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='t'"
        )
        assert len(result) == 1

    def test_execute_invalid_sql_raises(self, db: SQLiteAdapter) -> None:
        with pytest.raises(DatabaseError):
            db.execute("INVALID SQL")


class TestSQLiteAdapterInsert:
    def test_insert_single_row(self, db: SQLiteAdapter) -> None:
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        result = db.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result is not None
        assert result["name"] == "Alice"
        assert result["age"] == 30


class TestSQLiteAdapterBulkInsert:
    def test_bulk_insert_multiple_rows(self, db: SQLiteAdapter) -> None:
        db.bulk_insert(
            "users",
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
        )
        results = db.fetch_all("SELECT * FROM users ORDER BY id")
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Bob"

    def test_bulk_insert_empty_list(self, db: SQLiteAdapter) -> None:
        db.bulk_insert("users", [])
        results = db.fetch_all("SELECT * FROM users")
        assert len(results) == 0


class TestSQLiteAdapterFetchOne:
    def test_fetch_one_returns_dict(self, db: SQLiteAdapter) -> None:
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        result = db.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result == {"id": 1, "name": "Alice", "age": 30}

    def test_fetch_one_returns_none_when_empty(self, db: SQLiteAdapter) -> None:
        result = db.fetch_one("SELECT * FROM users WHERE id = 999")
        assert result is None


class TestSQLiteAdapterFetchAll:
    def test_fetch_all_returns_list_of_dicts(self, db: SQLiteAdapter) -> None:
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        results = db.fetch_all("SELECT * FROM users ORDER BY id")
        assert len(results) == 2
        assert results[0]["name"] == "Alice"

    def test_fetch_all_empty_table(self, db: SQLiteAdapter) -> None:
        results = db.fetch_all("SELECT * FROM users")
        assert results == []


class TestSQLiteAdapterFetchDf:
    def test_fetch_df_returns_dataframe(self, db: SQLiteAdapter) -> None:
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        df = db.fetch_df("SELECT * FROM users")
        assert len(df) == 1
        assert list(df.columns) == ["id", "name", "age"]

    def test_fetch_df_empty_result(self, db: SQLiteAdapter) -> None:
        df = db.fetch_df("SELECT * FROM users")
        assert len(df) == 0


class TestSQLiteAdapterUpdate:
    def test_update_row(self, db: SQLiteAdapter) -> None:
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        db.update("users", {"name": "Alicia"}, {"id": 1})
        result = db.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result is not None
        assert result["name"] == "Alicia"


class TestSQLiteAdapterDelete:
    def test_delete_row(self, db: SQLiteAdapter) -> None:
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        db.delete("users", {"id": 1})
        result = db.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result is None


class TestSQLiteAdapterTransactions:
    def test_begin_commit(self, db: SQLiteAdapter) -> None:
        db.begin()
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        db.commit()
        result = db.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result is not None

    def test_rollback(self, db: SQLiteAdapter) -> None:
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        # Use conn directly to avoid auto-commit in execute()
        cur = db.conn.cursor()
        cur.execute("DELETE FROM users WHERE id = 1")
        db.rollback()
        result = db.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result is not None


class TestSQLiteAdapterCopyExpert:
    def test_copy_expert_loads_file(self, db: SQLiteAdapter, tmp_path) -> None:
        sql_file = tmp_path / "data.sql"
        sql_file.write_text(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);"
        )
        db.copy_expert("", str(sql_file))
        result = db.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result is not None
        assert result["name"] == "Alice"
