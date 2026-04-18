from enum import Enum


class MailStatus(Enum):
    """Mail notification status types."""

    START = "Début"
    SUCCESS = "Succès"
    ERROR = "Erreur"
    SKIP = "Skip"
    WARNING = "Warning"
    INFO = "Information"


class MailPriority(Enum):
    """Mail priority levels."""

    NORMAL = 0
    LOW = 1
    HIGH = 2
