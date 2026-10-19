import logging
from dataclasses import dataclass
from typing import Any

from modules.infra.http_client.base import HttpInterface


@dataclass()
class SupersetApiClient:
    http_client: HttpInterface
    host: str
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
            url=f"{self.host}/api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider": "db",
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def list_datasets(self, token: str, page_size: int = 100) -> list[dict[str, Any]]:
        """
        List all datasets in Superset (handles pagination).

        Args:
            token: The user token for Superset
            page_size: Number of items per page (default 100)

        Returns:
            A list of all dataset objects
        """
        datasets: list[dict[str, Any]] = []
        page = 1

        while True:
            response = self.http_client.get(
                url=f"{self.host}/api/v1/dataset/",
                params={"page": page, "page_size": page_size},
                headers={"Authorization": f"Bearer {token}"},
            )
            response.raise_for_status()
            body = response.json()
            datasets.extend(body["result"])

            if len(datasets) >= body["count"] or not body["result"]:
                break
            page += 1

        return datasets

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
            url=f"{self.host}/api/v1/dataset/{dataset_id}/refresh",
            headers={"Authorization": f"Bearer {token}"},
        )
        response.raise_for_status()
