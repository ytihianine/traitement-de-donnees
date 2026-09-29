"""Output serialization adapters for pipeline operation results."""

import json
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

import pandas as pd

from modules.domain.dataset.ports import DatasetLocationProvider


# ================
# Output formats
# ================
@dataclass(frozen=True)
class DataframeOutput:
    """DataFrame value produced by a pipeline operation."""

    value: pd.DataFrame


@dataclass(frozen=True)
class JsonOutput:
    """JSON value produced by a pipeline operation."""

    value: dict


@dataclass(frozen=True)
class SqlOutput:
    """SQL statement produced by a pipeline operation."""

    value: str


# ================
# Output adapters
# ================
class OutputAdapter(ABC):
    """Serialize a supported pipeline output and delegate its storage."""

    @abstractmethod
    def supports(self, output: object) -> bool:
        """Return whether this adapter can serialize the output."""

    @abstractmethod
    def write(
        self,
        output: object,
        provider: DatasetLocationProvider,
        location: str,
    ) -> None:
        """Serialize the output and store the resulting content."""


class DataFrameOutputAdapter(OutputAdapter):
    """Serialize pandas DataFrames as Parquet content."""

    def supports(self, output: object) -> bool:
        """Return whether the output is a pandas DataFrame."""
        return isinstance(output, (pd.DataFrame, DataframeOutput))

    def write(
        self,
        output: object,
        provider: DatasetLocationProvider,
        location: str,
    ) -> None:
        """Serialize a DataFrame to Parquet bytes and store them."""
        if isinstance(output, DataframeOutput):
            dataframe = output.value
        elif isinstance(output, pd.DataFrame):
            dataframe = output
        else:
            raise TypeError("DataFrameOutputAdapter only supports pandas DataFrame or DataframeOutput outputs")

        content = dataframe.to_parquet(path=None, index=False)
        if not isinstance(content, bytes):
            raise TypeError("DataFrame serialization must produce bytes")

        provider.write(content=content, location=location)


class JsonOutputAdapter(OutputAdapter):
    """Serialize JsonOutput values as UTF-8 JSON content."""

    def supports(self, output: object) -> bool:
        """Return whether the output is a JsonOutput."""
        return isinstance(output, JsonOutput)

    def write(
        self,
        output: object,
        provider: DatasetLocationProvider,
        location: str,
    ) -> None:
        """Serialize a JsonOutput to UTF-8 JSON bytes and store them."""
        if not isinstance(output, JsonOutput):
            raise TypeError("JsonOutputAdapter only supports JsonOutput outputs")

        content = json.dumps(output.value).encode("utf-8")
        provider.write(content=content, location=location)


class SqlOutputAdapter(OutputAdapter):
    """Serialize SqlOutput values as UTF-8 SQL content."""

    def supports(self, output: object) -> bool:
        """Return whether the output is a SqlOutput."""
        return isinstance(output, SqlOutput)

    def write(
        self,
        output: object,
        provider: DatasetLocationProvider,
        location: str,
    ) -> None:
        """Serialize a SqlOutput to UTF-8 bytes and store them."""
        if not isinstance(output, SqlOutput):
            raise TypeError("SqlOutputAdapter only supports SqlOutput outputs")

        provider.write(content=output.value.encode("utf-8"), location=location)


class OutputAdapterRegistry:
    """Select an output adapter for a pipeline operation result."""

    def __init__(self, adapters: Sequence[OutputAdapter]) -> None:
        """Initialize the registry with adapters ordered by precedence."""
        self._adapters = tuple(adapters)

    def get_adapter(self, output: object) -> OutputAdapter:
        """Return the adapter that supports the output or raise a clear error."""
        for adapter in self._adapters:
            if adapter.supports(output):
                return adapter

        raise TypeError(f"No output adapter found for type {type(output).__name__}")


DEFAULT_OUTPUT_ADAPTER_REGISTRY = OutputAdapterRegistry(
    adapters=(
        DataFrameOutputAdapter(),
        JsonOutputAdapter(),
        SqlOutputAdapter(),
    )
)
