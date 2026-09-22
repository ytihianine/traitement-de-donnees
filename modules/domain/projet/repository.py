"""ProjectRepository port: read access to project configuration and metadata."""

from abc import ABC, abstractmethod

from modules.domain.projet.model import Contact, Documentation, ProjetMetadata, ProjetS3


class ProjectRepository(ABC):
    """Read access to project configuration and snapshot metadata.

    Implementations live outside the domain (see
    ``modules.infra.project.PostgresProjectRepository``).
    """

    @abstractmethod
    def get_list_contact(self, nom_projet: str) -> list[Contact]:
        """Get the contacts declared for a project."""

    @abstractmethod
    def get_list_documentation(self, nom_projet: str) -> list[Documentation]:
        """Get the documentation entries declared for a project."""

    @abstractmethod
    def get_projet_s3_info(self, nom_projet: str) -> ProjetS3:
        """Get the S3 storage configuration for a project.

        Raises:
            ValueError: If no S3 configuration is found.
        """

    @abstractmethod
    def get_projet_metadata(self, nom_projet: str, dag_completed: bool = False) -> ProjetMetadata:
        """Get the latest snapshot metadata for a project.

        Args:
            nom_projet: Project name.
            dag_completed: If True, only consider snapshots whose DAG run completed.

        Raises:
            ValueError: If no matching snapshot is found.
        """
