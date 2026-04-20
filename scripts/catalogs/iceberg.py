import pandas as pd
from src.infracatalog.iceberg import IcebergCatalog, generate_catalog_properties
from src._enums.filesystem import IcebergTableStatus
from scripts.settings import get_settings

settings = get_settings()

# CREDENTIALS
CATALOG_URI = settings.catalog.uri
CATALOG_WAREHOUSE = settings.catalog.warehouse
CATALOG_NAME = settings.catalog.name
CLIENT_ID = settings.catalog.client_id
CLIENT_SECRET = settings.catalog.client_secret
S3_ACCESS_KEY_ID = settings.s3.access_key_id
S3_ACCESS_SECRET_KEY = settings.s3.access_secret_key
S3_ENDPOINT = settings.s3.endpoint

additional_options = {
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": S3_ACCESS_KEY_ID,
    "s3.secret-access-key": S3_ACCESS_SECRET_KEY,
    "s3.region": "us-east-1",
}

NAMESPACE = "test_namespace"
TABLE = "test_table"
PATTERN = None


def create_test_table(catalog: IcebergCatalog) -> None:
    df = pd.DataFrame(data={"id": [1, 2, 3], "value": ["a", "b", "c"]})
    catalog.write_table_and_namespace(
        df=df,
        table_status=IcebergTableStatus.PROD,
        namespace=NAMESPACE,
        table_name=TABLE,
    )

    df_staging = pd.DataFrame(data={"id": [1, 2, 3], "value": ["a", "b", "c"]})
    catalog.write_table_and_namespace(
        df=df_staging,
        table_status=IcebergTableStatus.STAGING,
        namespace=NAMESPACE,
        table_name=TABLE,
    )


def read_table(catalog: IcebergCatalog) -> None:
    raw_data = catalog.read_table(table_name=f"{NAMESPACE}.{TABLE}")
    print(raw_data)

    df_tbl = catalog.read_table_as_df(table_name=f"{NAMESPACE}.{TABLE}")
    print(df_tbl.head())


def drop_tables(catalog: IcebergCatalog) -> None:
    # Get all tables
    iceberg_tbl_to_drop = catalog.list_tables(
        namespace=NAMESPACE,
        pattern=PATTERN,
    )
    print(
        f"Tables in namespace '{NAMESPACE}' with pattern '{PATTERN}': \n{iceberg_tbl_to_drop}"
    )

    # Drop staging tables from Iceberg catalog
    for table in iceberg_tbl_to_drop:
        print(f"Dropping table {table} ...")
        catalog.drop_table(table_name=".".join(table), purge=False)
        print(f"Table {table} dropped successfully !")


if __name__ == "__main__":
    # Init catalog
    props = generate_catalog_properties(
        uri=CATALOG_URI,
        warehouse=CATALOG_WAREHOUSE,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        options=additional_options,
    )

    print(f"Catalog properties: {props}")

    catalog = IcebergCatalog(
        name=CATALOG_NAME,
        properties=props,
    )
    print("Catalog loaded successfully!")

    # # Create test table
    create_test_table(catalog)

    # # Read table
    # read_table(catalog)

    # # Drop tables
    # drop_tables(catalog)
