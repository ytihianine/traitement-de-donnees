"""Configuration task group for retrieving project configuration at runtime."""

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from airflow.sdk import chain, task, task_group

from modules.domain.dataset.model import Dataset
from modules.infra.airflow.dag import AirflowDagRepository
from modules.infra.database.repository.dataset_context import DbDatasetContextRepository
from modules.infra.database.repository.projet import DbProjetRepository


def _check_nom_projet(nom_projet: str | None, context: dict[str, Any]) -> str:
    if nom_projet is None:
        nom_projet = AirflowDagRepository().get_project_name(context=context)
    return nom_projet


@task()
def get_documentation_task(nom_projet: str | None = None, **context) -> list[Mapping[str, Any]]:
    """Task to fetch project documentation at runtime."""
    projet_repository = DbProjetRepository()
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)
    docs = projet_repository.get_list_documentation(nom_projet=nom_projet)
    return [asdict(obj=doc) for doc in docs]


@task()
def get_contact_task(nom_projet: str | None = None, **context) -> list[Mapping[str, Any]]:
    """Task to fetch project contacts at runtime."""
    projet_repository = DbProjetRepository()
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)
    contacts = projet_repository.get_list_contact(nom_projet=nom_projet)
    return [asdict(obj=contact) for contact in contacts]


@task()
def get_source_fichier_task(nom_projet: str | None = None, **context) -> list[str]:
    """Task to fetch file source configurations at runtime."""
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)
    dataset_repository = DbDatasetContextRepository()
    sources = dataset_repository.get_list_source_fichier(nom_projet=nom_projet)
    return sources


@task()
def get_projet_datasets(nom_projet: str | None = None, **context) -> list[Dataset]:
    """Task to fetch the project selecteur configurations."""
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)
    dataset_repository = DbDatasetContextRepository()
    datasets_context = dataset_repository.get_list(nom_projet=nom_projet)

    return [Dataset(name=dataset_context.dataset.name) for dataset_context in datasets_context]


@task_group()
def config_projet_group(nom_projet: str | None = None, **context) -> None:
    """
    Groupe de tâches pour récupérer la configuration du projet

    Args:
        nom_projet (str | None): Le nom du projet

    Returns:
        None
    """
    return chain(
        [
            get_documentation_task(nom_projet=nom_projet, context=context),
            get_contact_task(nom_projet=nom_projet, context=context),
            get_source_fichier_task(nom_projet=nom_projet, context=context),
            get_projet_datasets(
                nom_projet=nom_projet,
            ),
        ]
    )
