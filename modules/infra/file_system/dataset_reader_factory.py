"""Factory for creating dataset readers from a StorageInfo."""

from modules.domain.dataset.model import StorageInfo, TypeSource
from modules.domain.dataset.ports import DatasetReader
from modules.infra.file_system.data_readers import FileDatasetReader
from modules.infra.file_system.factory import FSConfig
from modules.infra.grist.reader import GristReaderStrategy


def create_dataset_reader(storage_info: StorageInfo) -> DatasetReader:
    """Create the appropriate dataset reader for the given storage info.

    Args:
        storage_info: Storage info describing the dataset source

    Returns:
        A DatasetReader instance matching the source type

    Raises:
        ValueError: If the source type is not supported
    """
    if storage_info.type_source == TypeSource.GRIST:
        return GristReaderStrategy(
            fs_config=FSConfig(
                bucket=storage_info.bucket,
                connection_id=storage_info.s3_conn_id,
            ),
        )

    if storage_info.type_source == TypeSource.FILE:
        return FileDatasetReader(
            fs_config=FSConfig(
                bucket=storage_info.bucket,
                connection_id=storage_info.s3_conn_id,
            ),
        )

    raise ValueError(f"Unsupported source type: {storage_info.type_source}")
