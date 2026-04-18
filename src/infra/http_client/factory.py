"""Factory for creating HTTP clients."""

from src.enums.http import HttpHandlerType

from src.infra.http_client.adapters import HttpxClient, RequestsClient
from src.infra.http_client.base import HttpInterface
from src.infra.http_client.config import ClientConfig


def create_http_client(
    client_type: HttpHandlerType, config: ClientConfig
) -> HttpInterface:
    if client_type == HttpHandlerType.REQUEST:
        return RequestsClient(config=config)

    if client_type == HttpHandlerType.HTTPX:
        return HttpxClient(config)

    raise ValueError(
        f"Unsupported handler type: '{client_type}'. "
        f"Supported types: 'REQUESTS', 'HTTPX'"
    )
