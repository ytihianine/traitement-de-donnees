from enum import Enum, auto


class DagStatus(Enum):
    """DAG status"""

    RUN = auto()
    DEV = auto()
