"""Tests for S3FS (S3 file handler) using a mocked boto3 client."""

import io
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from infra.file_system.s3 import S3FS
from infra.file_system.exceptions import FileHandlerError, FileNotFoundError


class FakeClientExceptions:
    """Fake exceptions namespace for the mock client."""

    NoSuchKey = type("NoSuchKey", (Exception,), {})


def _make_handler(files: dict[str, bytes] | None = None) -> S3FS:
    """Create an S3FS with a mocked boto3 client backed by an in-memory dict."""
    store = dict(files) if files else {}
    client = MagicMock()
    client.exceptions = FakeClientExceptions()

    def head_object(Bucket: str, Key: str) -> dict:
        if Key not in store:
            raise client.exceptions.NoSuchKey("not found")
        return {
            "ContentLength": len(store[Key]),
            "LastModified": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "ETag": '"abc123"',
            "ContentType": "application/octet-stream",
        }

    def get_object(Bucket: str, Key: str) -> dict:
        if Key not in store:
            raise client.exceptions.NoSuchKey("not found")
        body = MagicMock()
        body.read.return_value = store[Key]
        return {"Body": body}

    def put_object(Bucket: str, Key: str, Body: object, **kwargs: object) -> None:
        if hasattr(Body, "read"):
            store[Key] = Body.read()  # type: ignore
        elif isinstance(Body, bytes):
            store[Key] = Body
        else:
            store[Key] = str(Body).encode()

    def delete_object(Bucket: str, Key: str) -> None:
        store.pop(Key, None)

    def copy_object(Bucket: str, Key: str, CopySource: dict) -> None:
        src_key = CopySource["Key"]
        if src_key not in store:
            raise client.exceptions.NoSuchKey("not found")
        store[Key] = store[src_key]

    def paginate(Bucket: str, Prefix: str) -> list[dict]:
        contents = [{"Key": k} for k in store if k.startswith(Prefix)]
        return [{"Contents": contents}] if contents else [{}]

    client.head_object = MagicMock(side_effect=head_object)
    client.get_object = MagicMock(side_effect=get_object)
    client.put_object = MagicMock(side_effect=put_object)
    client.delete_object = MagicMock(side_effect=delete_object)
    client.copy_object = MagicMock(side_effect=copy_object)

    paginator = MagicMock()
    paginator.paginate = MagicMock(side_effect=paginate)
    client.get_paginator = MagicMock(return_value=paginator)

    handler = S3FS(bucket="test-bucket", client=client)
    handler._store = store  # expose for assertions   # type: ignore
    return handler


class TestInit:
    def test_with_client(self) -> None:
        client = MagicMock()
        h = S3FS(bucket="b", client=client)
        assert h.bucket == "b"
        assert h.client is client

    def test_with_connection_id(self) -> None:
        h = S3FS(bucket="b", connection_id="conn")
        assert h.connection_id == "conn"

    def test_neither_raises(self) -> None:
        with pytest.raises(ValueError, match="Either"):
            S3FS(bucket="b")

    def test_both_raises(self) -> None:
        with pytest.raises(ValueError, match="only one"):
            S3FS(bucket="b", connection_id="c", client=MagicMock())


class TestWriteAndRead:
    def test_write_and_read_bytes(self) -> None:
        h = _make_handler()
        h.write("file.bin", b"hello")
        result = h.read("file.bin")
        assert result.read() == b"hello"

    def test_write_and_read_string(self) -> None:
        h = _make_handler()
        h.write("file.txt", "bonjour")
        result = h.read("file.txt")
        assert result.read() == b"bonjour"

    def test_write_and_read_binary_io(self) -> None:
        h = _make_handler()
        h.write("file.bin", io.BytesIO(b"stream"))
        result = h.read("file.bin")
        assert result.read() == b"stream"

    def test_read_missing_raises(self) -> None:
        h = _make_handler()
        with pytest.raises(FileNotFoundError):
            h.read("missing.txt")


class TestExists:
    def test_exists_true(self) -> None:
        h = _make_handler({"key.txt": b"x"})
        assert h.exists("key.txt") is True

    def test_exists_false(self) -> None:
        h = _make_handler()
        assert h.exists("nope.txt") is False


class TestDelete:
    def test_delete_existing(self) -> None:
        h = _make_handler({"file.txt": b"x"})
        h.delete("file.txt")
        assert h.exists("file.txt") is False

    def test_delete_single(self) -> None:
        h = _make_handler({"file.txt": b"x"})
        h.delete_single("file.txt")
        assert h.exists("file.txt") is False


class TestGetMetadata:
    def test_metadata_fields(self) -> None:
        h = _make_handler({"data.csv": b"col1,col2"})
        meta = h.get_metadata("data.csv")
        assert meta.name == "data.csv"
        assert meta.size == len(b"col1,col2")
        assert meta.checksum == "abc123"

    def test_metadata_missing_raises(self) -> None:
        h = _make_handler()
        with pytest.raises((FileNotFoundError, FileHandlerError)):
            h.get_metadata("missing.csv")


class TestListFiles:
    def test_list_files(self) -> None:
        h = _make_handler({"dir/a.txt": b"a", "dir/b.txt": b"b", "other/c.txt": b"c"})
        files = h.list_files("dir")
        assert sorted(files) == ["dir/a.txt", "dir/b.txt"]

    def test_list_files_empty(self) -> None:
        h = _make_handler()
        files = h.list_files("empty")
        assert files == []

    def test_list_files_with_pattern(self) -> None:
        h = _make_handler({"dir/a.txt": b"a", "dir/b.csv": b"b"})
        files = h.list_files("dir", pattern="*.txt")
        assert len(files) == 1
        assert files[0] == "dir/a.txt"


class TestMove:
    def test_move(self) -> None:
        h = _make_handler({"txt": b"data"})
        h.move("txt", "dst.txt")
        assert h.exists("txt") is False
        assert h.read("dst.txt").read() == b"data"

    def test_move_missing_source_raises(self) -> None:
        h = _make_handler()
        with pytest.raises(FileNotFoundError):
            h.move("missing.txt", "dst.txt")


class TestCopy:
    def test_copy(self) -> None:
        h = _make_handler({"txt": b"data"})
        h.copy("txt", "dst.txt")
        assert h.exists("txt") is True
        assert h.read("dst.txt").read() == b"data"

    def test_copy_missing_source_raises(self) -> None:
        h = _make_handler()
        with pytest.raises(FileNotFoundError):
            h.copy("missing.txt", "dst.txt")
