from abc import ABC, abstractmethod


class SelecteurRepository(ABC):

    @abstractmethod
    def get_list_source_fichier(self, nom_projet: str): ...

    @abstractmethod
    def get_storage_info(self, nom_projet: str, selecteur: str): ...

    @abstractmethod
    def get_list_selecteur_storage_info(self, nom_projet: str, selecteur: str): ...

    @abstractmethod
    def column_mapping_dataframe(self, nom_projet: str, selecteur: str): ...
