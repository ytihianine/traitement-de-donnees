"""Factory for creating HTTP clients."""

from enum import Enum, auto

from modules.infra.http_client.adapters import HttpxClient, RequestsClient
from modules.infra.http_client.base import HttpInterface
from modules.infra.http_client.config import ClientConfig


class HttpHandlerType(Enum):
    """Http handler types enumeration."""

    REQUEST = auto()
    HTTPX = auto()


def create_http_client(client_type: HttpHandlerType, config: ClientConfig) -> HttpInterface:
    if client_type == HttpHandlerType.REQUEST:
        return RequestsClient(config=config)

    if client_type == HttpHandlerType.HTTPX:
        return HttpxClient(config)

    raise ValueError(f"Unsupported handler type: '{client_type}'. " f"Supported types: 'REQUESTS', 'HTTPX'")
