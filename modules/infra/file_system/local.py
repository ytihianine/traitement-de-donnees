"""Local filesystem implementation of file handler."""

import logging
import mimetypes
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO

from .base import FileMetadata, FSInterface


@dataclass
class LocalFS(FSInterface):
    """Handler for local filesystem operations."""

    base_path: Path

    def read(self, file_path: str | Path, validate: bool = True) -> BinaryIO:
        """Read file content from local filesystem."""
        abs_path = self.get_absolute_path(file_path)
        if validate:
            self.validate(abs_path)
        return open(abs_path, "rb")

    def write(self, file_path: str | Path, content: str | bytes | BinaryIO) -> None:
        """Write content to local filesystem."""
        abs_path = self.get_absolute_path(file_path)
        # Create directory if it doesn't exist
        abs_path.parent.mkdir(parents=True, exist_ok=True)

        # Temporary file in the same directory
        with tempfile.NamedTemporaryFile(delete=False, dir=abs_path.parent) as tmp:
            tmp_path = Path(tmp.name)

            try:
                if isinstance(content, str):
                    tmp.write(content.encode("utf-8"))
                elif isinstance(content, bytes):
                    tmp.write(content)
                else:
                    shutil.copyfileobj(content, tmp)  # type: ignore

                tmp.flush()
                os.fsync(tmp.fileno())
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise

        # Atomic replace (guaranteed on POSIX)
        os.replace(tmp_path, abs_path)

    def delete(self, file_path: str | Path) -> None:
        """Delete file from local filesystem."""
        abs_path = self.get_absolute_path(file_path)
        if abs_path.exists():
            logging.info(msg=f"Deleting file at : {abs_path}")
            os.remove(abs_path)
        else:
            logging.info(msg=f"File does not exists at : {abs_path}")

    def delete_single(self, file_path: str | Path) -> None:
        """Delete file from local filesystem."""
        self.delete(file_path=file_path)

    def exists(self, file_path: str | Path) -> bool:
        """Check if file exists in local filesystem."""
        return self.get_absolute_path(file_path).exists()

    def get_metadata(self, file_path: str | Path) -> FileMetadata:
        """Get metadata for local file."""
        abs_path = self.get_absolute_path(file_path)
        if not abs_path.exists():
            raise FileNotFoundError(f"File not found: {abs_path}")

        stat = abs_path.stat()
        mime_type, _ = mimetypes.guess_type(str(abs_path))

        return FileMetadata(
            name=abs_path.name,
            size=stat.st_size,
            created_at=datetime.fromtimestamp(stat.st_ctime),
            modified_at=datetime.fromtimestamp(stat.st_mtime),
            mime_type=mime_type or "application/octet-stream",
            checksum="",  # self.validator.calculate_checksum(abs_path),
            extra={
                "permissions": oct(stat.st_mode)[-3:],
                "owner": stat.st_uid,
                "group": stat.st_gid,
            },
        )

    def list_files(self, directory: str | Path, pattern: str | None = None) -> list[str]:
        """List files in local directory."""
        abs_path = self.get_absolute_path(directory)
        if not abs_path.exists():
            raise FileNotFoundError(f"Directory not found: {abs_path}")
        if not abs_path.is_dir():
            raise TypeError(f"Not a directory: {abs_path}")

        if pattern:
            return [str(p) for p in abs_path.glob(pattern)]
        return [str(p) for p in abs_path.iterdir() if p.is_file()]

    def move(self, source: str | Path, destination: str | Path) -> None:
        """Move file in local filesystem."""
        src_path = self.get_absolute_path(source)
        dst_path = self.get_absolute_path(destination)

        if not src_path.exists():
            raise FileNotFoundError(f"Source file not found: {src_path}")

        # Create destination directory if it doesn't exist
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src_path), str(dst_path))

    def copy(self, source: str | Path, destination: str | Path) -> None:
        """Copy file in local filesystem."""
        src_path = self.get_absolute_path(source)
        dst_path = self.get_absolute_path(destination)

        if not src_path.exists():
            raise FileNotFoundError(f"Source file not found: {src_path}")

        # Create destination directory if it doesn't exist
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src_path), str(dst_path))
