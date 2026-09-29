from unittest.mock import Mock

import pandas as pd
import pytest
from modules.domain.pipeline.output import (
    DEFAULT_OUTPUT_ADAPTER_REGISTRY,
    DataFrameOutputAdapter,
    JsonOutput,
    JsonOutputAdapter,
    SqlOutput,
    SqlOutputAdapter,
)


def test_dataframe_output_adapter_serializes_to_parquet_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    output = pd.DataFrame({"value": [1]})
    provider = Mock()

    def to_parquet(dataframe: pd.DataFrame, path: None, index: bool) -> bytes:
        assert dataframe is output
        assert path is None
        assert not index
        return b"parquet-content"

    monkeypatch.setattr(pd.DataFrame, "to_parquet", to_parquet)

    DataFrameOutputAdapter().write(output=output, provider=provider, location="exports/data.parquet")

    provider.write.assert_called_once_with(content=b"parquet-content", location="exports/data.parquet")


def test_json_output_adapter_serializes_to_utf8_bytes() -> None:
    provider = Mock()

    JsonOutputAdapter().write(
        output=JsonOutput({"foo": "bar"}),
        provider=provider,
        location="exports/data.json",
    )

    provider.write.assert_called_once_with(content=b'{"foo": "bar"}', location="exports/data.json")


def test_sql_output_adapter_serializes_to_utf8_bytes() -> None:
    provider = Mock()

    SqlOutputAdapter().write(
        output=SqlOutput("SELECT 1"),
        provider=provider,
        location="exports/query.sql",
    )

    provider.write.assert_called_once_with(content=b"SELECT 1", location="exports/query.sql")


@pytest.mark.parametrize(
    ("output", "adapter_type"),
    [
        (pd.DataFrame({"value": [1]}), DataFrameOutputAdapter),
        (JsonOutput({"foo": "bar"}), JsonOutputAdapter),
        (SqlOutput("SELECT 1"), SqlOutputAdapter),
    ],
)
def test_default_output_adapter_registry_selects_supported_adapter(output: object, adapter_type: type) -> None:
    assert isinstance(DEFAULT_OUTPUT_ADAPTER_REGISTRY.get_adapter(output), adapter_type)


class UnsupportedOutput:
    pass


def test_default_output_adapter_registry_rejects_unsupported_output() -> None:
    with pytest.raises(TypeError, match="No output adapter found for type UnsupportedOutput"):
        DEFAULT_OUTPUT_ADAPTER_REGISTRY.get_adapter(UnsupportedOutput())
