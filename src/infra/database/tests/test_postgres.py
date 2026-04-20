"""Tests for PgAdapter."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from infra.database.exceptions import DatabaseError
from infra.database.postgres import PgAdapter


class TestPgAdapterInit:
    def test_init_with_connection_id(self) -> None:
        adapter = PgAdapter(connection_id="my_conn")
        assert adapter.connection_id == "my_conn"
        assert adapter._use_airflow is True

    def test_init_with_local_params(self) -> None:
        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        assert adapter._use_airflow is False
        assert adapter._local_params["host"] == "localhost"
        assert adapter._local_params["port"] == 5432

    def test_init_raises_if_both_modes(self) -> None:
        with pytest.raises(ValueError, match="not both"):
            PgAdapter(connection_id="my_conn", host="localhost")

    def test_init_raises_if_no_mode(self) -> None:
        with pytest.raises(ValueError):
            PgAdapter()


class TestPgAdapterGetUri:
    def test_get_uri_local_masks_password(self) -> None:
        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="secret"
        )
        uri = adapter.get_uri()
        assert "****" in uri
        assert "secret" not in uri
        assert "postgresql://" in uri


class TestPgAdapterHook:
    def test_hook_raises_in_local_mode(self) -> None:
        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        with pytest.raises(RuntimeError, match="not available in local mode"):
            _ = adapter.hook


class TestPgAdapterExecute:
    @patch("infra.database.postgres.psycopg2")
    def test_execute_local_mode(self, mock_psycopg2: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn

        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        adapter.execute("SELECT 1")

        mock_cursor.execute.assert_called_once_with("SELECT 1", None)
        mock_conn.commit.assert_called_once()

    @patch("infra.database.postgres.psycopg2")
    def test_execute_raises_database_error(self, mock_psycopg2: MagicMock) -> None:
        mock_psycopg2.connect.side_effect = Exception("connection failed")

        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        with pytest.raises(DatabaseError):
            adapter.execute("SELECT 1")


class TestPgAdapterFetchOne:
    @patch("infra.database.postgres.psycopg2")
    def test_fetch_one_returns_dict(self, mock_psycopg2: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchone.return_value = (1, "Alice")
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn

        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        result = adapter.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result == {"id": 1, "name": "Alice"}

    @patch("infra.database.postgres.psycopg2")
    def test_fetch_one_returns_none_when_empty(self, mock_psycopg2: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchone.return_value = None
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn

        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        result = adapter.fetch_one("SELECT * FROM users WHERE id = 999")
        assert result is None


class TestPgAdapterFetchAll:
    @patch("infra.database.postgres.psycopg2")
    def test_fetch_all_returns_list_of_dicts(self, mock_psycopg2: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn

        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        results = adapter.fetch_all("SELECT * FROM users")
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Alice"}


class TestPgAdapterFetchDf:
    @patch("infra.database.postgres.pd.read_sql")
    @patch("infra.database.postgres.create_engine")
    def test_fetch_df_local_mode(
        self, mock_engine: MagicMock, mock_read_sql: MagicMock
    ) -> None:
        expected_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        mock_read_sql.return_value = expected_df

        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        df = adapter.fetch_df("SELECT * FROM users")
        assert len(df) == 1
        mock_read_sql.assert_called_once()


class TestPgAdapterInsert:
    @patch.object(PgAdapter, "execute")
    def test_insert_calls_execute(self, mock_execute: MagicMock) -> None:
        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        adapter.insert("users", {"id": 1, "name": "Alice"})
        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        assert "INSERT INTO users" in call_args[0][0]


class TestPgAdapterBulkInsert:
    @patch("infra.database.postgres.psycopg2")
    def test_bulk_insert_empty_list(self, mock_psycopg2: MagicMock) -> None:
        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        adapter.bulk_insert("users", [])  # should return early

    @patch("infra.database.postgres.psycopg2")
    def test_bulk_insert_multiple_rows(self, mock_psycopg2: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_psycopg2.connect.return_value = mock_conn

        adapter = PgAdapter(
            host="localhost", dbname="testdb", user="user", password="pass"
        )
        adapter.bulk_insert(
            "users", [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        )
        mock_cursor.executemany.assert_called_once()
