from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from modules.domain.dataset.model import TypeSource


@dataclass(frozen=True)
class StorageInfo:
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

    def __post_init__(self) -> None:
        if not isinstance(self.type_source, TypeSource) and self.type_source is not None:
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


class StorageInfoProvider(ABC):
    @abstractmethod
    def get_by_dataset(self, nom_projet: str, dataset_name: str) -> StorageInfo: ...

    @abstractmethod
    def get_by_projet(self, nom_projet: str) -> list[StorageInfo]: ...

    @abstractmethod
    def get_list_source_fichier(self, nom_projet: str) -> list[str]: ...
