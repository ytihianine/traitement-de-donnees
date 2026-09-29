"""YAML adapter for the DatasetContextRepository port."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from modules.domain.dataset.model import Dataset, DatasetContext, DatasetLocation, StageLocation
from modules.domain.dataset.repository import DatasetContextRepository
from modules.domain.projet.model import Projet


@dataclass(frozen=True)
class YamlDatasetContextRepository(DatasetContextRepository):
    """DatasetContextRepository backed by a declarative YAML file instead of the ``conf_projets`` schema."""

    yaml_path: str | Path

    def _load_projects(self) -> list[Mapping[str, Any]]:
        path = Path(self.yaml_path)
        if not path.exists():
            raise FileNotFoundError(f"Dataset context YAML file not found: {path}")

        with open(file=path, encoding="utf-8") as f:
            data = yaml.safe_load(stream=f)

        if data is None:
            return []

        projets = data.get("projet", [])
        if not isinstance(projets, list):
            raise ValueError(f"Top-level 'projet' key must be a list, got {type(projets).__name__}")
        return projets

    def _find_project(self, nom_projet: str) -> Mapping[str, Any]:
        for projet in self._load_projects():
            if projet.get("name") == nom_projet:
                return projet
        raise ValueError(f"No project found with name {nom_projet}")

    @staticmethod
    def _datasets_of(projet: Mapping[str, Any], nom_projet: str) -> list[Mapping[str, Any]]:
        datasets = projet.get("datasets", [])
        if not isinstance(datasets, list):
            raise ValueError(f"'datasets' of project {nom_projet} must be a list, got {type(datasets).__name__}")
        return datasets

    @staticmethod
    def _build_context(projet: Mapping[str, Any], dataset: Mapping[str, Any]) -> DatasetContext:
        locations = {}
        for location in dataset.get("locations", []):
            locations[StageLocation(location["stage"])] = DatasetLocation(**location)

        return DatasetContext(
            projet=Projet(name=projet["name"]),
            dataset=Dataset(name=dataset["name"]),
            location=locations,
        )

    def get_list(self, nom_projet: str) -> list[DatasetContext]:
        projet = self._find_project(nom_projet=nom_projet)
        datasets = self._datasets_of(projet=projet, nom_projet=nom_projet)
        return [self._build_context(projet=projet, dataset=dataset) for dataset in datasets]

    def get(self, nom_projet: str, nom_dataset: str) -> DatasetContext:
        projet = self._find_project(nom_projet=nom_projet)
        for dataset in self._datasets_of(projet=projet, nom_projet=nom_projet):
            if dataset.get("name") == nom_dataset:
                return self._build_context(projet=projet, dataset=dataset)
        raise ValueError(f"No dataset found with name {nom_dataset} in project {nom_projet}")

    def get_list_source_fichier(self, nom_projet: str) -> list[str]:
        projet = self._find_project(nom_projet=nom_projet)
        sources: list[str] = []
        for dataset in self._datasets_of(projet=projet, nom_projet=nom_projet):
            location = (dataset.get("src_location") or {}).get("location")
            if location:
                sources.append(location)
        return sources

    def get_list_column_mapping(self, nom_projet: str, dataset_name: str) -> Sequence[Mapping[str, Any]]:
        # Column mappings are not part of the declarative YAML format.
        return []
