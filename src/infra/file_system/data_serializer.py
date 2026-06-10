"""Data Serializer for deserializing bytes into different data structures."""

import io
from abc import ABC, abstractmethod
from enum import Enum
from typing import TypeVar, Generic

import pandas as pd

T = TypeVar(name="T")


class SerializationFormat(str, Enum):
    """Supported serialization formats."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    EXCEL = "excel"
    AUTO = "auto"


class DataSerializer(ABC, Generic[T]):
    """Abstract base class for data serialization."""

    @abstractmethod
    def load(self, buffer: io.BytesIO, **kwargs) -> T: ...

    @abstractmethod
    def dump(self, data: T, **kwargs) -> bytes: ...


class CSVSerializer(DataSerializer[pd.DataFrame]):
    """Serializer for converting CSV bytes / DataFrame."""

    def load(self, buffer: io.BytesIO, **kwargs) -> pd.DataFrame:
        return pd.read_csv(buffer, **kwargs)

    def dump(self, data: pd.DataFrame, **kwargs) -> bytes:
        buf = io.StringIO()
        data.to_csv(buf, index=False, **kwargs)
        return buf.getvalue().encode(encoding="utf-8")


class ParquetSerializer(DataSerializer[pd.DataFrame]):
    """Serializer for converting PARQUET bytes / DataFrame."""

    def load(self, buffer: io.BytesIO, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(path=buffer, **kwargs)

    def dump(self, data: pd.DataFrame, **kwargs) -> bytes:
        buf = io.BytesIO()
        data.to_parquet(buf, index=False, **kwargs)
        return buf.getvalue()


class ExcelSerializer(DataSerializer[pd.DataFrame]):
    """Serializer for converting EXCEL bytes / DataFrame."""

    def load(self, buffer: io.BytesIO, **kwargs) -> pd.DataFrame:
        return pd.read_excel(buffer, **kwargs)

    def dump(self, data: pd.DataFrame, **kwargs) -> bytes:
        buf = io.BytesIO()
        with pd.ExcelWriter(path=buf, engine="xlsxwriter") as writer:  # type: ignore
            data.to_excel(excel_writer=writer, index=False)

        return buf.getvalue()


class JSONSerializer(DataSerializer[pd.DataFrame]):
    """Serializer for converting JSON bytes / DataFrame."""

    def load(self, buffer: io.BytesIO, **kwargs) -> pd.DataFrame:
        return pd.read_json(path_or_buf=buffer, **kwargs)

    def dump(self, data: pd.DataFrame, **kwargs) -> bytes:
        return data.to_json(orient="records").encode(encoding="utf-8")  # type: ignore
