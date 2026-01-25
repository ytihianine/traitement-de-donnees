from dataclasses import dataclass


@dataclass(frozen=True)
class DbInfo:
    projet: str
    selecteur: str
    tbl_name: str
    tbl_order: int
    is_partitionned: bool
    partition_period: str
    load_strategy: str


@dataclass(frozen=True)
class SourceGrist:
    projet: str
    selecteur: str
    type_source: str
    id_source: str


@dataclass(frozen=True)
class SourceFichier:
    projet: str
    selecteur: str
    type_source: str
    id_source: str
    bucket: str
    s3_key: str
    filepath_source_s3: str


@dataclass(frozen=True)
class ProjetS3:
    projet: str
    bucket: str
    key: str
    key_tmp: str


@dataclass(frozen=True)
class SelecteurInfo:
    projet: str
    selecteur: str
    # s3 info
    filename: str
    s3_key: str
    bucket: str
    projet_s3_key: str
    projet_s3_key_tmp: str
    filepath_s3: str
    filepath_tmp_s3: str
    # db info
    tbl_name: str
    tbl_order: int
    is_partitionned: bool
    partition_period: str
    load_strategy: str


@dataclass(frozen=True)
class SelecteurS3:
    projet: str
    selecteur: str
    filename: str
    s3_key: str
    bucket: str
    projet_s3_key: str
    projet_s3_key_tmp: str
    filepath_s3: str
    filepath_tmp_s3: str


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
    is_generic: bool
