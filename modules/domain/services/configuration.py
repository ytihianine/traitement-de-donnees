from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from modules.domain.models.projet import Contact, Documentation
from modules.domain.models.selecteurs import SelecteurConfig
from modules.infra.database.base import DBInterface


@dataclass(frozen=True)
class ConfigurationProvider(ABC):

    @abstractmethod
    def get(self, selecteur_id: str) -> SelecteurConfig: ...

    @abstractmethod
    def get_all(self) -> list[SelecteurConfig]: ...

    @abstractmethod
    def get_list_source_fichier(self) -> list[str]: ...

    @abstractmethod
    def get_list_contact(self) -> list[Contact]: ...

    @abstractmethod
    def get_list_documentation(self) -> list[Documentation]: ...


@dataclass(frozen=True)
class YAMLConfigurationProvider(ConfigurationProvider):
    file_path: Path

    def get(self, selecteur_id: str) -> SelecteurConfig:
        raise NotImplementedError("YAMLConfigurationProvider.get is not wired yet")

    def get_all(self) -> list[SelecteurConfig]:
        raise NotImplementedError("YAMLConfigurationProvider.get_all is not wired yet")

    def get_list_source_fichier(self) -> list[str]:
        raise NotImplementedError("YAMLConfigurationProvider.get_list_source_fichier is not wired yet")

    def get_list_contact(self) -> list[Contact]:
        raise NotImplementedError("YAMLConfigurationProvider.get_list_contact is not wired yet")

    def get_list_documentation(self) -> list[Documentation]:
        raise NotImplementedError("YAMLConfigurationProvider.get_list_documentation is not wired yet")


@dataclass(frozen=True)
class PostgresConfigurationProvider(ConfigurationProvider):
    db: DBInterface

    def get(self, selecteur_id: str) -> SelecteurConfig:
        raise NotImplementedError("PostgresConfigurationProvider.get is not wired yet")

    def get_all(self) -> list[SelecteurConfig]:
        raise NotImplementedError("PostgresConfigurationProvider.get_all is not wired yet")

    def get_list_source_fichier(self) -> list[str]:
        raise NotImplementedError("PostgresConfigurationProvider.get_list_source_fichier is not wired yet")

    def get_list_contact(self) -> list[Contact]:
        raise NotImplementedError("PostgresConfigurationProvider.get_list_contact is not wired yet")

    def get_list_documentation(self) -> list[Documentation]:
        raise NotImplementedError("PostgresConfigurationProvider.get_list_documentation is not wired yet")
