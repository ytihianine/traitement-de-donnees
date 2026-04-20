"""Tests for LocalFS (local filesystem handler)."""

from pathlib import Path

import pytest

from src.infrafile_system.local import LocalFS
from src.infrafile_system.exceptions import (
    FileNotFoundError,
)


@pytest.fixture
def tmp_base(tmp_path: Path) -> Path:
    """Provide a temporary base directory."""
    return tmp_path


@pytest.fixture
def handler(tmp_base: Path) -> LocalFS:
    """Provide a LocalFS handler with a temp base path."""
    return LocalFS(base_path=tmp_base)


class TestInit:
    def test_base_path_set(self, tmp_base: Path) -> None:
        h = LocalFS(base_path=tmp_base)
        assert h.base_path == tmp_base

    def test_no_base_path(self) -> None:
        h = LocalFS()
        assert h.base_path is None


class TestWriteAndRead:
    def test_write_bytes(self, handler: LocalFS, tmp_base: Path) -> None:
        handler.write("file.bin", b"hello")
        assert (tmp_base / "file.bin").read_bytes() == b"hello"

    def test_write_string(self, handler: LocalFS, tmp_base: Path) -> None:
        handler.write("file.txt", "bonjour")
        assert (tmp_base / "file.txt").read_bytes() == b"bonjour"

    def test_write_binary_io(self, handler: LocalFS, tmp_base: Path) -> None:
        import io

        handler.write("file.bin", io.BytesIO(b"stream"))
        assert (tmp_base / "file.bin").read_bytes() == b"stream"

    def test_write_creates_subdirectories(
        self, handler: LocalFS, tmp_base: Path
    ) -> None:
        handler.write("sub/dir/file.txt", b"nested")
        assert (tmp_base / "sub" / "dir" / "file.txt").read_bytes() == b"nested"

    def test_read_returns_binary_io(self, handler: LocalFS) -> None:
        handler.write("file.txt", b"content")
        result = handler.read("file.txt")
        assert result.read() == b"content"

    def test_read_missing_file_raises(self, handler: LocalFS) -> None:
        with pytest.raises(FileNotFoundError):
            handler.read("missing.txt")


class TestDelete:
    def test_delete_existing(self, handler: LocalFS) -> None:
        handler.write("file.txt", b"x")
        handler.delete("file.txt")
        assert not handler.exists("file.txt")

    def test_delete_missing_does_not_raise(self, handler: LocalFS) -> None:
        handler.delete("nonexistent.txt")  # should not raise

    def test_delete_single(self, handler: LocalFS) -> None:
        handler.write("file.txt", b"x")
        handler.delete_single("file.txt")
        assert not handler.exists("file.txt")


class TestExists:
    def test_exists_true(self, handler: LocalFS) -> None:
        handler.write("file.txt", b"x")
        assert handler.exists("file.txt") is True

    def test_exists_false(self, handler: LocalFS) -> None:
        assert handler.exists("nope.txt") is False


class TestGetMetadata:
    def test_metadata_fields(self, handler: LocalFS) -> None:
        handler.write("data.csv", b"col1,col2\n1,2")
        meta = handler.get_metadata("data.csv")
        assert meta.name == "data.csv"
        assert meta.size == len(b"col1,col2\n1,2")
        assert meta.mime_type == "text/csv"
        assert "permissions" in meta.extra
        assert "owner" in meta.extra

    def test_metadata_missing_raises(self, handler: LocalFS) -> None:
        with pytest.raises(FileNotFoundError):
            handler.get_metadata("missing.csv")


class TestListFiles:
    def test_list_files_in_directory(self, handler: LocalFS, tmp_base: Path) -> None:
        handler.write("dir/a.txt", b"a")
        handler.write("dir/b.txt", b"b")
        files = handler.list_files("dir")
        basenames = sorted(Path(f).name for f in files)
        assert basenames == ["a.txt", "b.txt"]

    def test_list_files_with_pattern(self, handler: LocalFS, tmp_base: Path) -> None:
        handler.write("dir/a.txt", b"a")
        handler.write("dir/b.csv", b"b")
        files = handler.list_files("dir", pattern="*.txt")
        assert len(files) == 1
        assert Path(files[0]).name == "a.txt"

    def test_list_files_missing_dir_raises(self, handler: LocalFS) -> None:
        with pytest.raises(FileNotFoundError):
            handler.list_files("nonexistent")


class TestMove:
    def test_move_file(self, handler: LocalFS) -> None:
        handler.write("txt", b"data")
        handler.move("txt", "dst.txt")
        assert not handler.exists("txt")
        assert handler.read("dst.txt").read() == b"data"

    def test_move_creates_dest_dir(self, handler: LocalFS) -> None:
        handler.write("txt", b"data")
        handler.move("txt", "sub/dst.txt")
        assert handler.read("sub/dst.txt").read() == b"data"

    def test_move_missing_source_raises(self, handler: LocalFS) -> None:
        with pytest.raises(FileNotFoundError):
            handler.move("missing.txt", "dst.txt")


class TestCopy:
    def test_copy_file(self, handler: LocalFS) -> None:
        handler.write("txt", b"data")
        handler.copy("txt", "dst.txt")
        assert handler.exists("txt")
        assert handler.read("dst.txt").read() == b"data"

    def test_copy_creates_dest_dir(self, handler: LocalFS) -> None:
        handler.write("txt", b"data")
        handler.copy("txt", "sub/dst.txt")
        assert handler.read("sub/dst.txt").read() == b"data"

    def test_copy_missing_source_raises(self, handler: LocalFS) -> None:
        with pytest.raises(FileNotFoundError):
            handler.copy("missing.txt", "dst.txt")


class TestValidate:
    def test_validate_existing(self, handler: LocalFS) -> None:
        handler.write("file.txt", b"ok")
        assert handler.validate("file.txt") is True

    def test_validate_missing_raises(self, handler: LocalFS) -> None:
        with pytest.raises(FileNotFoundError):
            handler.validate("missing.txt")
