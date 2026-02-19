import logging
from dataclasses import dataclass, field
from typing import Any, Mapping
import pyarrow as pa

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table
from airflow.sdk import Variable
import pandas as pd


def generate_catalog_properties(
    uri: str,
    warehouse: str = "data_store",
    client_id: str | None = None,
    client_secret: str | None = None,
) -> Mapping[str, Any]:
    client_id = (
        Variable.get(key="iceberg_client_id") if client_id is None else client_id
    )
    client_secret = (
        Variable.get(key="iceberg_client_secret")
        if client_secret is None
        else client_secret
    )

    properties = (
        {
            "uri": uri,
            "warehouse": warehouse,
            "credential": f"{client_id}:{client_secret}",
            "scope": "PRINCIPAL_ROLE:ALL",
            "header.X-Iceberg-Access-Delegation": None,
        },
    )

    return properties


@dataclass
class IcebergCatalog:
    name: str
    properties: Mapping[str, Any]
    catalog: Catalog = field(init=False)

    def __post_init__(self) -> None:
        self.catalog = self._load_catalog()

    def _load_catalog(self) -> Catalog:
        # Logic to load the Iceberg catalog using the provided properties
        return load_catalog(name=self.name, **self.properties)

    def _get_schema_from_dataframe(self, df: pd.DataFrame) -> pa.Schema:
        # Convertir le DataFrame en schéma PyArrow
        return pa.Schema.from_pandas(df)

    def create_namespace(self, namespace: str) -> None:
        logging.info(msg=f"Creating namespace with name: {namespace}")
        self.catalog.create_namespace_if_not_exists(namespace=namespace)

    def create_table(
        self, table_name: str, df: pd.DataFrame, location: str | None = None
    ) -> Table:
        # Générer le schéma à partir de la structure du DataFrame
        logging.info(msg=f"Creating table with name: {table_name}")
        table = self.catalog.create_table_if_not_exists(
            identifier=table_name,
            schema=self._get_schema_from_dataframe(df=df),
            # location=location
        )
        return table

    def update_table(self, table_name: str, df: pd.DataFrame) -> Table:
        # Générer le schéma à partir de la structure du DataFrame
        logging.info(msg=f"Update table with name: {table_name}")
        table = self.catalog.load_table(identifier=table_name)
        new_schema = self._get_schema_from_dataframe(df=df)
        with table.update_schema() as update:
            update.union_by_name(new_schema)
        return table

    def write_table(
        self, table_name: str, df: pd.DataFrame, overwrite: bool = False
    ) -> None:
        # Logic to write data to a table in the Iceberg catalog
        table = self.update_table(table_name=table_name, df=df)

        logging.info(msg=f"Writing to table with name: {table_name}")
        pa_data = pa.Table.from_pandas(df, preserve_index=False)
        if overwrite:
            table.overwrite(df=pa_data)
        else:
            table.append(df=pa_data)
        return

    def read_table(self, table_name: str) -> pd.DataFrame:
        # Logic to read data from a table in the Iceberg catalog
        logging.info(msg=f"Reading from table with name: {table_name}")
        table = self.catalog.load_table(identifier=table_name)
        df = table.scan().to_pandas()
        return df
