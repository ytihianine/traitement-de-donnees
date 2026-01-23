from enum import Enum, auto


class DagStatus(Enum):
    """DAG status"""

    RUN = auto()
    DEV = auto()


class TypeDocumentation(Enum):
    """Type de documentation"""

    PIPELINE = "pipeline"
    DATA = "data"
