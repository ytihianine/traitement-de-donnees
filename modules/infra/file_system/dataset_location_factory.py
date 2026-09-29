"""Factory for creating dataset readers from a DatasetLocation."""

import logging

from modules.domain.dataset.model import DatasetLocation, TypeLocation
from modules.domain.dataset.ports import DatasetLocationProvider, DatasetLocationProviderFactory
from modules.infra.file_system.dataset_location import (
    DbDatasetLocationProvider,
    GristDatasetLocationProvider,
    LocalFileDatasetLocationProvider,
    S3DatasetLocationProvider,
)


def _create_s3(location: DatasetLocation) -> DatasetLocationProvider:
    if location.conn_id is None:
        raise ValueError("conn_id is required for S3")

    return S3DatasetLocationProvider(conn_id=location.conn_id)


def _create_database(location: DatasetLocation) -> DatasetLocationProvider:
    if location.conn_id is None:
        raise ValueError("conn_id is required for database")

    return DbDatasetLocationProvider(conn_id=location.conn_id)


def _create_local(location: DatasetLocation) -> DatasetLocationProvider:
    return LocalFileDatasetLocationProvider()


def _create_grist(location: DatasetLocation) -> DatasetLocationProvider:
    return GristDatasetLocationProvider()


class DatasetLocationFactory(DatasetLocationProviderFactory):
    def create(self, dataset_location: DatasetLocation) -> DatasetLocationProvider:
        """Create the appropriate dataset location provider for the given storage info.

        Args:
            dataset_location: Storage info describing the dataset source

        Returns:
            A DatasetLocationProvider instance matching the source type

        Raises:
            ValueError: If the source type is not supported
        """
        factories = {
            TypeLocation.S3_FILE: _create_s3,
            TypeLocation.DB: _create_database,
            TypeLocation.LOCAL_FILE: _create_local,
            TypeLocation.GRIST: _create_grist,
        }

        logging.info(msg=f"Instantiating DatasetLocation provider of type {dataset_location.type_location}")
        provider_factory = factories.get(dataset_location.type_location)
        if provider_factory is None:
            raise ValueError(f"Unsupported source type: {dataset_location.type_location}")
        logging.info(msg="DatasetLocation provider instantiated")

        return provider_factory(dataset_location)
