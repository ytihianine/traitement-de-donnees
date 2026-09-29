from pathlib import Path

import pytest
import yaml
from modules.domain.dataset.model import TypeLocation
from modules.infra.yaml.dataset_context import YamlDatasetContextRepository


def _write_yaml(tmp_path: Path, projet_name: str, datasets: list[dict]) -> str:
    payload = {"projet": [{"name": projet_name, "datasets": datasets}]}
    yaml_path = tmp_path / "dataset_context.yaml"
    yaml_path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
    return str(yaml_path)


def _dataset(name: str, src_location: str = "src_doc") -> dict:
    return {
        "name": name,
        "src_location": {"type_location": "grist", "location": src_location, "conn_id": None},
        "tmp_location": {"type_location": "s3", "location": None, "conn_id": None},
        "dest_location": {"type_location": "database", "location": None, "conn_id": None},
    }


def test_get_list_returns_all_datasets(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(
        yaml_path=_write_yaml(tmp_path, "Mon projet", [_dataset("alpha"), _dataset("beta")])
    )

    result = repo.get_list(nom_projet="Mon projet")

    assert [ctx.dataset_name for ctx in result] == ["alpha", "beta"]


def test_get_returns_matching_dataset(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(
        yaml_path=_write_yaml(tmp_path, "Mon projet", [_dataset("alpha"), _dataset("beta")])
    )

    ctx = repo.get(nom_projet="Mon projet", nom_dataset="beta")

    assert ctx.dataset_name == "beta"
    assert ctx.projet_name == "Mon projet"
    assert ctx.projet.id is None
    assert ctx.src_location.type_location is TypeLocation.GRIST
    assert ctx.src_location.location == "src_doc"


def test_get_list_coerces_type_location_enum(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(yaml_path=_write_yaml(tmp_path, "Mon projet", [_dataset("alpha")]))

    ctx = repo.get_list(nom_projet="Mon projet")[0]

    assert ctx.tmp_location.type_location is TypeLocation.S3_FILE
    assert ctx.dest_location.type_location is TypeLocation.DB


def test_get_list_source_fichier_collects_non_empty_locations(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(
        yaml_path=_write_yaml(tmp_path, "Mon projet", [_dataset("alpha"), _dataset("beta", src_location="beta_src")])
    )

    sources = repo.get_list_source_fichier(nom_projet="Mon projet")

    assert sources == ["src_doc", "beta_src"]


def test_get_list_column_mapping_returns_empty(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(yaml_path=_write_yaml(tmp_path, "Mon projet", [_dataset("alpha")]))

    assert repo.get_list_column_mapping(nom_projet="Mon projet", dataset_name="alpha") == []


def test_get_list_raises_on_unknown_project(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(yaml_path=_write_yaml(tmp_path, "Mon projet", [_dataset("alpha")]))

    with pytest.raises(ValueError, match="No project found with name Inconnu"):
        repo.get_list(nom_projet="Inconnu")


def test_get_raises_on_unknown_dataset(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(yaml_path=_write_yaml(tmp_path, "Mon projet", [_dataset("alpha")]))

    with pytest.raises(ValueError, match="No dataset found with name zeta in project Mon projet"):
        repo.get(nom_projet="Mon projet", nom_dataset="zeta")


def test_missing_yaml_file_raises(tmp_path: Path) -> None:
    repo = YamlDatasetContextRepository(yaml_path=tmp_path / "does_not_exist.yaml")

    with pytest.raises(FileNotFoundError):
        repo.get_list(nom_projet="Mon projet")


def test_default_path_points_to_repo_yaml() -> None:
    repo = YamlDatasetContextRepository()

    assert Path(repo.yaml_path).name == "dataset_context.yaml"
    assert Path(repo.yaml_path).parent.name == "configuration_projets"
