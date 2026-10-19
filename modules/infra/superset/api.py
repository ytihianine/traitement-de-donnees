import logging
from dataclasses import dataclass

from modules.infra.http_client.base import HttpInterface


@dataclass()
class SupersetApiClient:
    http_client: HttpInterface
    username: str
    password: str

    def get_user_token(self, username: str, password: str) -> str:
        """
        Get the user token for Superset.

        Args:
            username: The username of the Superset user
            password: The password of the Superset user

        Returns:
            The user token as a string
        """
        response = self.http_client.post(
            url="/api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider": "db",
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def list_datasets(self, token: str) -> list[int]:
        """
        List datasets in Superset.

        Args:
            token: The user token for Superset

        Returns:
            A list of dataset IDs as integers
        """
        response = self.http_client.get(
            url="/api/v1/dataset/",
            headers={"Authorization": f"Bearer {token}"},
        )
        response.raise_for_status()
        result = response.json()["result"]
        return [dataset["id"] for dataset in result]

    def sync_dataset_columns(self, dataset_id: int, token: str) -> None:
        """
        Sync dataset columns in Superset.

        Args:
            dataset_id: The ID of the dataset in Superset
            token: The user token for Superset

        Returns:
            None
        """
        logging.info(msg=f"Refreshing dataset columns for dataset_id={dataset_id}.")
        response = self.http_client.put(
            url=f"/api/v1/dataset/{dataset_id}/refresh",
            headers={"Authorization": f"Bearer {token}"},
        )
        response.raise_for_status()
