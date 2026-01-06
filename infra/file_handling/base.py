"""Base interface for file handling operations."""

from abc import ABC, abstractmethod
from typing import Any, BinaryIO, Dict, List, Optional, Union
from pathlib import Path
from datetime import datetime

from .exceptions import (
    FileNotFoundError,
)


class FileMetadata:
    """Class to hold file metadata."""

    def __init__(
        self,
        name: str,
        size: int,
        created_at: datetime,
        modified_at: datetime,
        mime_type: str,
        checksum: str,
        extra: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.size = size
        self.created_at = created_at
        self.modified_at = modified_at
        self.mime_type = mime_type
        self.checksum = checksum
        self.extra = extra or {}


class BaseFileHandler(ABC):
    """Abstract base class for file handling operations."""

    def __init__(self, base_path: Optional[Union[str, Path]] = None):
        self.base_path = Path(base_path) if base_path else None

    @abstractmethod
    def read(self, file_path: Union[str, Path], validate: bool = True) -> BinaryIO:
        """Read file content."""
        pass

    @abstractmethod
    def write(
        self, file_path: Union[str, Path], content: Union[str, bytes, BinaryIO]
    ) -> None:
        """Write content to file."""
        pass

    @abstractmethod
    def delete(self, file_path: Union[str, Path]) -> None:
        """Delete file."""
        pass

    @abstractmethod
    def exists(self, file_path: Union[str, Path]) -> bool:
        """Check if file exists."""
        pass

    @abstractmethod
    def get_metadata(self, file_path: Union[str, Path]) -> FileMetadata:
        """Get file metadata."""
        pass

    @abstractmethod
    def list_files(
        self, directory: Union[str, Path], pattern: Optional[str] = None
    ) -> List[str]:
        """List files in directory."""
        pass

    @abstractmethod
    def move(self, source: Union[str, Path], destination: Union[str, Path]) -> None:
        """Move file from source to destination."""
        pass

    @abstractmethod
    def copy(self, source: Union[str, Path], destination: Union[str, Path]) -> None:
        """Copy file from source to destination."""
        pass

    def validate(
        self,
        file_path: Union[str, Path],
        allowed_types: Optional[List[str]] = None,
        max_size: Optional[int] = None,
    ) -> bool:
        """Validate file against various criteria."""
        if not self.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        return True

    def get_absolute_path(self, file_path: Union[str, Path]) -> Path:
        """Convert relative path to absolute path."""
        path = Path(file_path)
        if self.base_path and not path.is_absolute():
            return self.base_path / path
        return path
