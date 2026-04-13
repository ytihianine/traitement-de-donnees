import requests

from infra.catalog.polaris import PolarisCatalog
from scripts.settings import get_settings

settings = get_settings()

# CREDENTIALS
S3_ACCESS_KEY_ID = settings.s3.access_key_id
S3_ACCESS_SECRET_KEY = settings.s3.access_secret_key
CLIENT_ID = settings.catalog.client_id
CLIENT_SECRET = settings.catalog.client_secret

POLARIS_URL = settings.polaris.url
REALM = settings.polaris.realm
CATALOG_NAME = settings.polaris.catalog_name
PRINCIPAL_NAME = settings.polaris.principal_name
PRINCIPAL_ROLE_NAME = settings.polaris.principal_role_name
CATALOG_ROLE_NAME = settings.polaris.catalog_role_name
ca_bundle = settings.polaris.ca_bundle

polaris_client = PolarisCatalog(url=POLARIS_URL, realm=REALM)

# Get root token
token = polaris_client.get_root_access_token(
    client_id=CLIENT_ID, client_secret=CLIENT_SECRET
)
print("✓ Token retrieved")

# Delete existing catalog if it exists
delete_url = f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}"
# polaris_client.get_catalog_info(token=token, catalog_name=CATALOG_NAME)  # Check if catalog exists
try:
    polaris_client.delete_catalog(
        token=token, catalog_name=CATALOG_NAME
    )  # Delete if exists
except Exception as e:
    print(f"✓ No existing catalog to delete: {e}")

# Create catalog with credentials
try:
    polaris_client.create_catalog(
        token=token, catalog_name=CATALOG_NAME, storage_config={}
    )
    print("✓ Catalog created")
except Exception as e:
    print(f"✗ Catalog creation failed - might already exists: {e}")


# Create principal
client_id, client_secret = polaris_client.create_principal(
    token, principal_name=PRINCIPAL_NAME
)
# Export credentials to .env file
credentials_file = (
    "/home/onyxia/work/traitement-des-donnees/scripts/catalogs/.env.polaris"
)
with open(file=credentials_file, mode="w") as f:
    f.write(f"POLARIS_CLIENT_ID={client_id}\n")
    f.write(f"POLARIS_CLIENT_SECRET={client_secret}\n")
print(f"✓ Principal created, credentials saved to {credentials_file}")

# Create roles
try:
    polaris_client.create_principal_role(token, role_name=PRINCIPAL_ROLE_NAME)
except Exception:
    print("✓ Principal role already exists")

try:
    polaris_client.create_catalog_role(
        token, catalog_name=CATALOG_NAME, role_name=CATALOG_ROLE_NAME
    )
    print("✓ Roles created")
except Exception:
    print("✓ Catalog role already exists")

# Assign roles
polaris_client.assign_principal_role(
    token, principal_name=PRINCIPAL_NAME, role_name=PRINCIPAL_ROLE_NAME
)
polaris_client.assign_catalog_role(
    token,
    principal_role_name=PRINCIPAL_ROLE_NAME,
    catalog_name=CATALOG_NAME,
    catalog_role_name=CATALOG_ROLE_NAME,
)
print("✓ Roles assigned")

# Grant ALL privileges
grant_url = f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles/{CATALOG_ROLE_NAME}/grants"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
    "Polaris-Realm": REALM,
}

privileges = [
    "CATALOG_MANAGE_CONTENT",
    "NAMESPACE_CREATE",
    "NAMESPACE_FULL_METADATA",
    "TABLE_CREATE",
    "TABLE_WRITE_DATA",
    "TABLE_READ_DATA",
    "TABLE_FULL_METADATA",
]

for priv in privileges:
    requests.put(
        url=grant_url,
        headers=headers,
        json={"type": "catalog", "privilege": priv},
        verify=False,
    ).raise_for_status()
    print(f"  ✓ Granted {priv}")
