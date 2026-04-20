from enum import Enum, auto


class HttpHandlerType(Enum):
    """Http handler types enumeration."""

    REQUEST = auto()
    HTTPX = auto()
