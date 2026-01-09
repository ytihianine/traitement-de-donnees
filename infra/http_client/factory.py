"""Factory for creating HTTP clients."""

from enums.http import HttpHandlerType

from .adapters import HttpxClient, RequestsClient
from .base import AbstractHTTPClient
from .config import ClientConfig


def create_http_client(
    client_type: HttpHandlerType, config: ClientConfig
) -> AbstractHTTPClient:
    if client_type == HttpHandlerType.REQUEST:
        return RequestsClient(config=config)

    if client_type == HttpHandlerType.HTTPX:
        return HttpxClient(config)

    raise ValueError(
        f"Unsupported handler type: '{client_type}'. "
        f"Supported types: 'REQUESTS', 'HTTPX'"
    )
