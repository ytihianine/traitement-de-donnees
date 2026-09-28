from unittest.mock import Mock

import pytest
from modules.infra.file_system import dataset_location
from modules.infra.file_system.dataset_location import (
    DbDatasetLocationProvider,
    GristDatasetLocationProvider,
    LocalFileDatasetLocationProvider,
    S3DatasetLocationProvider,
)


def test_s3_provider_writes_received_bytes_without_transformation(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = Mock()
    monkeypatch.setattr(dataset_location, "create_file_handler", Mock(return_value=handler))

    S3DatasetLocationProvider(conn_id="s3_connection").write(
        content=b"serialized-content",
        location="bucket/path/output.parquet",
    )

    handler.write.assert_called_once_with(file_path="path/output.parquet", content=b"serialized-content")


def test_local_provider_writes_received_bytes_without_transformation(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = Mock()
    monkeypatch.setattr(dataset_location, "create_file_handler", Mock(return_value=handler))

    LocalFileDatasetLocationProvider().write(
        content=b"serialized-content",
        location="exports/output.json",
    )

    handler.write.assert_called_once_with(file_path="exports/output.json", content=b"serialized-content")


def test_database_provider_write_remains_unimplemented() -> None:
    with pytest.raises(NotImplementedError, match="DbDatasetWriter is not wired yet"):
        DbDatasetLocationProvider(conn_id="database_connection").write(
            content=b"serialized-content",
            location="schema.table",
        )


def test_grist_provider_write_remains_unimplemented() -> None:
    with pytest.raises(NotImplementedError, match="GristDatasetWriter is not wired yet"):
        GristDatasetLocationProvider().write(
            content=b"serialized-content",
            location="document.table",
        )
