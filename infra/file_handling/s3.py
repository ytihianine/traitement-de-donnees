"""S3 implementation of file handler using Airflow's S3 hooks."""

import io
import mimetypes
from pathlib import Path
from typing import BinaryIO, List, Optional, Union

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from .base import BaseFileHandler, FileMetadata
from .exceptions import FileHandlerError, FileNotFoundError


class S3FileHandler(BaseFileHandler):
    """Handler for S3 storage operations using Airflow's S3Hook."""

    def __init__(self, connection_id: str, bucket: str):
        """
        Initialize S3 file handler with Airflow connection.

        Args:
            connection_id: Airflow connection ID for S3
            bucket: S3 bucket name
        """
        super().__init__()
        self.connection_id = connection_id
        self.bucket = bucket
        self._hook = None

    @property
    def hook(self) -> S3Hook:
        """Lazy initialization of S3Hook."""
        if self._hook is None:
            self._hook = S3Hook(aws_conn_id=self.connection_id)
        return self._hook

    def read(self, file_path: Union[str, Path], validate: bool = True) -> BinaryIO:
        """Read file from S3 using Airflow's S3Hook."""
        key = str(file_path)
        try:
            print(f"Reading file from {key}")
            if validate:
                self.validate(key)

            if not self.exists(key):
                raise FileNotFoundError(f"File not found in S3: {key}")

            # Use S3Hook to read the file
            obj = self.hook.get_key(key, self.bucket)
            content = obj.get()["Body"].read()
            return io.BytesIO(content)

        except FileNotFoundError:
            raise
        except Exception as e:
            raise FileHandlerError(f"Error reading file from S3: {key}") from e

    def write(
        self,
        file_path: Union[str, Path],
        content: Union[str, bytes, BinaryIO],
        content_type: Optional[str] = None,
    ) -> None:
        """
        Write content to S3 using Airflow's S3Hook.

        Supports all file types: CSV, TSV, XLSX, Parquet, JSON, etc.

        Args:
            file_path: S3 key/path for the file
            content: Content to write (str, bytes, or file-like object)
            content_type: Optional MIME type. If not provided, will be inferred from file extension
        """
        key = str(file_path)
        try:
            print(f"Writing file to {key}")

            # Infer content type if not provided
            if content_type is None:
                content_type, _ = mimetypes.guess_type(key)
                if content_type is None:
                    content_type = "application/octet-stream"

            # Convert content to bytes if it's a string
            if isinstance(content, str):
                content_bytes = content.encode("utf-8")
                file_obj = io.BytesIO(initial_bytes=content_bytes)
            # If already bytes, wrap in BytesIO
            elif isinstance(content, bytes):
                file_obj = io.BytesIO(initial_bytes=content)
            # If it's already a file-like object, use it directly
            elif hasattr(content, "read"):
                file_obj = content
            else:
                raise ValueError(f"Unsupported content type: {type(content)}")

            # Ensure we're at the beginning of the stream
            if hasattr(file_obj, "seek"):
                file_obj.seek(0)

            # Upload to S3 with content type
            self.hook.load_file_obj(
                file_obj=file_obj,
                key=key,
                bucket_name=self.bucket,
                replace=True,
                encrypt=False,  # Set to True if you need encryption
                acl_policy=None,  # e.g., 'public-read' if needed
            )

            print(f"Successfully wrote {key} with content type: {content_type}")

        except Exception as e:
            raise FileHandlerError(f"Error writing file to S3: {key}") from e

    def delete(self, file_path: Union[str, Path]) -> None:
        """Delete file from S3 using Airflow's S3Hook."""
        key = str(file_path)
        try:
            self.hook.delete_objects(bucket=self.bucket, keys=[key])
        except Exception as e:
            raise FileHandlerError(f"Error deleting file from S3: {key}") from e

    def exists(self, file_path: Union[str, Path]) -> bool:
        """Check if file exists in S3 using Airflow's S3Hook."""
        key = str(file_path)
        try:
            return self.hook.check_for_key(key, self.bucket)
        except Exception:
            return False

    def get_metadata(self, file_path: Union[str, Path]) -> FileMetadata:
        """Get S3 file metadata using Airflow's S3Hook."""
        key = str(file_path)
        try:
            obj = self.hook.get_key(key, self.bucket)
            if not obj:
                raise FileNotFoundError(f"File not found in S3: {key}")

            mime_type, _ = mimetypes.guess_type(key)

            return FileMetadata(
                name=Path(key).name,
                size=obj.content_length,
                created_at=obj.last_modified,
                modified_at=obj.last_modified,
                mime_type=mime_type or obj.content_type or "application/octet-stream",
                checksum=obj.e_tag.strip('"'),
                extra={
                    "storage_class": obj.storage_class,
                    "version_id": obj.version_id,
                    "metadata": obj.metadata,
                },
            )
        except FileNotFoundError:
            raise
        except Exception as e:
            raise FileHandlerError(f"Error getting S3 metadata: {key}") from e

    def list_files(
        self, directory: Union[str, Path], pattern: Optional[str] = None
    ) -> List[str]:
        """List files in S3 directory using Airflow's S3Hook."""
        prefix = str(directory).rstrip("/") + "/"
        try:
            keys = self.hook.list_keys(bucket_name=self.bucket, prefix=prefix)

            if not keys:
                return []

            if pattern:
                from fnmatch import fnmatch

                return [k for k in keys if fnmatch(k, pattern)]
            return keys

        except Exception as e:
            raise FileHandlerError(f"Error listing S3 directory: {prefix}") from e

    def move(self, source: Union[str, Path], destination: Union[str, Path]) -> None:
        """Move/rename file in S3 using Airflow's S3Hook."""
        src_key = str(source)
        dst_key = str(destination)

        try:
            if not self.exists(src_key):
                raise FileNotFoundError(f"Source file not found in S3: {src_key}")

            # Copy then delete using S3Hook
            self.hook.copy_object(
                source_bucket_key=src_key,
                dest_bucket_key=dst_key,
                source_bucket_name=self.bucket,
                dest_bucket_name=self.bucket,
            )
            self.delete(src_key)
        except FileNotFoundError:
            raise
        except Exception as e:
            raise FileHandlerError(
                f"Error moving file in S3: {src_key} -> {dst_key}"
            ) from e

    def copy(self, source: Union[str, Path], destination: Union[str, Path]) -> None:
        """Copy file in S3 using Airflow's S3Hook."""
        src_key = str(source)
        dst_key = str(destination)

        try:
            if not self.exists(src_key):
                raise FileNotFoundError(f"Source file not found in S3: {src_key}")

            self.hook.copy_object(
                source_bucket_key=src_key,
                dest_bucket_key=dst_key,
                source_bucket_name=self.bucket,
                dest_bucket_name=self.bucket,
            )
        except FileNotFoundError:
            raise
        except Exception as e:
            raise FileHandlerError(
                f"Error copying file in S3: {src_key} -> {dst_key}"
            ) from e
