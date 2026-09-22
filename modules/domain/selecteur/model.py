from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any

from modules.constants import DEFAULT_PG_DATA_CONN_ID, DEFAULT_S3_CONN_ID
from modules.domain.selecteur.model import SelecteurConfig


# =================
# Enums
# =================
class TypeSource(Enum):
    """Type de source de données"""

    GRIST = "Grist"
    FILE = "Fichier"


class PartitionTimePeriod(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name.upper()

    DAY = auto()
    WEEK = auto()
    MONTH = auto()
    YEAR = auto()


class LoadStrategy(Enum):
    """Load strategies for data ingestion."""

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        return name.upper()

    FULL_LOAD = auto()
    INCREMENTAL = auto()
    APPEND = auto()


# =================
# Dataclasses
# =================
@dataclass(frozen=True, kw_only=True)
class ExecutionOptions:
    # S3
    s3_conn_id: str = DEFAULT_S3_CONN_ID
    write_to_s3: bool = True
    write_to_s3_with_iceberg: bool = True
    read_options: dict[str, Any] = field(default_factory=dict)
    # Database
    db_conn_id: str = DEFAULT_PG_DATA_CONN_ID
    write_to_db: bool = True
    use_prod_schema: bool = True
    tbl_order: int = 0
    keep_file_id_col: bool = True
    is_partitioned: bool = True
    partition_period: PartitionTimePeriod = PartitionTimePeriod.DAY
    load_strategy: LoadStrategy = LoadStrategy.APPEND

    def __post_init__(self) -> None:
        # Convert partition_period and load_strategy to their respective Enum types if they are provided as strings
        if not isinstance(self.partition_period, PartitionTimePeriod):
            object.__setattr__(
                self,
                "partition_period",
                PartitionTimePeriod(value=self.partition_period),
            )

        if not isinstance(self.load_strategy, LoadStrategy):
            object.__setattr__(self, "load_strategy", LoadStrategy(value=self.load_strategy))


@dataclass(frozen=True)
class SelecteurConfig:
    projet: str
    selecteur: str
    # s3 info
    s3_conn_id: str
    bucket: str
    s3_key: str
    filename: str
    local_dir: str
    # db info
    tbl_name: str | None
    # Source info
    type_source: TypeSource
    id_source: str | None
    # Execution options
    execution_options: ExecutionOptions

    def __post_init__(self) -> None:
        if not isinstance(self.type_source, TypeSource):
            object.__setattr__(self, "type_source", TypeSource(value=self.type_source))

    def get_full_s3_key(
        self,
        with_bucket: bool = False,
        with_tmp_segment: bool = False,
        use_id_source: bool = False,
    ) -> str:
        segments = [self.s3_key]
        if with_bucket:
            segments.insert(0, self.bucket)
        if with_tmp_segment:
            segments.append("tmp")

        if use_id_source and self.id_source is not None:
            segments.append(self.id_source)
        else:
            segments.append(self.filename)

        return "/".join(segments)

    def get_local_path(self) -> str:
        if self.filename is None:
            return str(Path(self.local_dir) / "filename_undefined")
        return str(Path(self.local_dir) / self.filename)

    def get_iceberg_namespace(self, with_bucket: bool = False) -> str:
        s3_key = self.get_full_s3_key(with_bucket=with_bucket)
        namespace_split = s3_key.split(sep=".")[0].split(sep="/")[:-1]
        return ".".join(namespace_split)

    @classmethod
    def load(
        cls,
        config: SelecteurConfig,
        execution_options: ExecutionOptions,
    ) -> "SelecteurConfig":
        return cls(
            **config.__dict__,
            execution_options=execution_options,
        )

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> "SelecteurConfig":
        return cls(
            **config,
        )
