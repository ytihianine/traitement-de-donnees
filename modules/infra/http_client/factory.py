"""Factory for creating HTTP clients."""

from enum import Enum

from modules.infra.http_client.adapters import HttpxClient, RequestsClient
from modules.infra.http_client.base import HttpInterface
from modules.infra.http_client.config import ClientConfig


class HttpHandlerType(Enum):
    """Http handler types enumeration."""

    REQUEST = "REQUESTS"
    HTTPX = "HTTPX"

    def serialize(self) -> str:
        return self.value

    def deserialize(self) -> "HttpHandlerType":
        return HttpHandlerType(self.value)


def create_http_client(client_type: HttpHandlerType, config: ClientConfig) -> HttpInterface:
    _registry = {
        HttpHandlerType.REQUEST: RequestsClient,
        HttpHandlerType.HTTPX: HttpxClient,
    }

    return _registry[client_type](config=config)
