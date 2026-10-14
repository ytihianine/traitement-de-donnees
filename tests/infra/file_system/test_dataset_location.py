from unittest.mock import Mock

import pandas as pd
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


def test_database_provider_writes_tsv_then_bulk_inserts(monkeypatch: pytest.MonkeyPatch) -> None:
    local_handler = Mock()
    db_handler = Mock()
    monkeypatch.setattr(dataset_location, "create_file_handler", Mock(return_value=local_handler))
    monkeypatch.setattr(dataset_location, "create_db_handler", Mock(return_value=db_handler))
    monkeypatch.setattr(
        dataset_location.pd,
        "read_parquet",
        Mock(return_value=pd.DataFrame({"id": [1], "name": ["alice"]})),
    )

    DbDatasetLocationProvider(conn_id="database_connection").write(
        content=b"serialized-parquet",
        location="schema.table",
    )

    local_handler.write.assert_called_once()
    write_kwargs = local_handler.write.call_args.kwargs
    assert str(write_kwargs["file_path"]).endswith("schema.table.tsv")
    assert write_kwargs["content"] == b"id\tname\n1\talice\n"

    db_handler.copy_expert.assert_called_once()
    copy_kwargs = db_handler.copy_expert.call_args.kwargs
    assert "COPY schema.table (id, name)" in copy_kwargs["sql"]
    assert copy_kwargs["filepath"].endswith("schema.table.tsv")


def test_grist_provider_write_remains_unimplemented() -> None:
    with pytest.raises(NotImplementedError, match="GristDatasetWriter is not wired yet"):
        GristDatasetLocationProvider().write(
            content=b"serialized-content",
            location="document.table",
        )
