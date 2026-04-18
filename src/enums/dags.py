from enum import Enum, auto


class DagStatus(Enum):
    """DAG status"""

    RUN = auto()
    DEV = auto()


class FeatureFlags(Enum):
    """Feature flags for conditional task execution"""

    DB = "db"
    MAIL = "mail"
    S3 = "s3"
    CONVERT_FILES = "convert_files"
    DOWNLOAD_GRIST_DOC = "download_grist_doc"


class TypeDocumentation(Enum):
    """Type de documentation"""

    PIPELINE = "pipeline"
    DATA = "data"


class TypeSource(Enum):
    """Type de source de données"""

    GRIST = "Grist"
    FILE = "Fichier"
