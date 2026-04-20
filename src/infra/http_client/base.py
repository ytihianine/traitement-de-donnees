from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import urljoin

from infra.http_client.config import ClientConfig
from infra.http_client.types import HTTPResponse


class HttpInterface(ABC):
    """Abstract base class for HTTP clients."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self._session = None

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint."""
        if self.config.base_url:
            return urljoin(self.config.base_url, endpoint)
        return endpoint

    @abstractmethod
    def request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        **kwargs,
    ) -> HTTPResponse:
        """Make an HTTP request."""
        raise NotImplementedError

    def get(
        self, endpoint: str, params: dict[str, Any] | None = None, **kwargs
    ) -> HTTPResponse:
        return self.request("GET", endpoint, params=params, **kwargs)

    def post(
        self,
        endpoint: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ) -> HTTPResponse:
        return self.request("POST", endpoint, data=data, json=json, **kwargs)

    def put(
        self,
        endpoint: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ) -> HTTPResponse:
        return self.request("PUT", endpoint, data=data, json=json, **kwargs)

    def patch(
        self,
        endpoint: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ) -> HTTPResponse:
        return self.request("PATCH", endpoint, data=data, json=json, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> HTTPResponse:
        return self.request("DELETE", endpoint, **kwargs)

    @abstractmethod
    def close(self) -> None:
        """Close the client and release resources."""
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
