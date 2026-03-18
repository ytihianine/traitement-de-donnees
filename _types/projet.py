from dataclasses import dataclass
from pathlib import Path

from enums.dags import TypeSource
from enums.database import LoadStrategy, PartitionTimePeriod
from utils.config.vars import DEFAULT_S3_CONN_ID, DEFAULT_PG_DATA_CONN_ID


@dataclass(frozen=True)
class ProjetS3:
    projet: str
    bucket: str
    key: str
    key_tmp: str


@dataclass(frozen=True)
class ColumnMapping:
    projet: str
    selecteur: str
    colname_source: str
    colname_dest: str


@dataclass(frozen=True)
class Documentation:
    projet: str
    type_documentation: str
    lien: str


@dataclass(frozen=True)
class Contact:
    projet: str
    contact_mail: str
    is_mail_generic: bool


# ==================
# Selecteur
# ==================
@dataclass(frozen=True)
class SelecteurStorageInfo:
    projet: str
    selecteur: str
    # s3 info
    bucket: str
    s3_key: str
    filename: str
    local_dir: str
    # db info
    tbl_name: str
    # Source info
    type_source: TypeSource
    id_source: str | None

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
            return str(Path(self.local_dir) / "filename_undefined")  # noqa
        return str(Path(self.local_dir) / self.filename)

    def get_iceberg_namespace(self, with_bucket: bool = False) -> str:
        s3_key = self.get_full_s3_key(with_bucket=with_bucket)
        namespace_split = s3_key.split(sep=".")[0].split(sep="/")[:-1]
        return ".".join(namespace_split)


@dataclass(frozen=True, kw_only=True)
class SelecteurStorageOptions:
    # S3
    s3_conn_id: str = DEFAULT_S3_CONN_ID
    write_to_s3: bool = True
    write_to_s3_with_iceberg: bool = True
    # Database
    db_conn_id: str = DEFAULT_PG_DATA_CONN_ID
    write_to_db: bool = True
    use_prod_schema: bool = True
    tbl_order: int = 0
    keep_file_id_col: bool = True
    is_partitioned: bool = True
    partition_period: PartitionTimePeriod = PartitionTimePeriod.DAY
    load_strategy: LoadStrategy = LoadStrategy.INCREMENTAL


@dataclass(frozen=True)
class SelecteurConfig:
    selecteur_info: SelecteurStorageInfo
    options: SelecteurStorageOptions

    @classmethod
    def load(
        cls, selecteur_info: SelecteurStorageInfo, options: SelecteurStorageOptions
    ) -> "SelecteurConfig":
        return cls(
            selecteur_info=selecteur_info,
            options=options,
        )
