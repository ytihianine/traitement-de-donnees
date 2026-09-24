"""Configuration task group for retrieving project configuration at runtime."""

from collections.abc import Mapping
from dataclasses import asdict
from typing import Any

from airflow.sdk import chain, task, task_group

from modules.infra.airflow.dag import AirflowDagRepository
from modules.infra.database.postgres.projet_repository import PostgresProjetRepository
from modules.types.projet import SelecteurStorageOptions, custom_asdict_factory


def _check_nom_projet(nom_projet: str | None, context: dict[str, Any]) -> str:
    if nom_projet is None:
        nom_projet = AirflowDagRepository().get_project_name(context=context)
    return nom_projet


@task()
def get_documentation_task(nom_projet: str | None = None, **context) -> list[Mapping[str, Any]]:
    """Task to fetch project documentation at runtime."""
    projet_repository = PostgresProjetRepository()
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)
    docs = projet_repository.get_list_documentation(nom_projet=nom_projet)
    return [asdict(obj=doc) for doc in docs]


@task()
def get_contact_task(nom_projet: str | None = None, **context) -> list[Mapping[str, Any]]:
    """Task to fetch project contacts at runtime."""
    projet_repository = PostgresProjetRepository()
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)
    contacts = projet_repository.get_list_contact(nom_projet=nom_projet)
    return [asdict(obj=contact) for contact in contacts]


@task()
def get_source_fichier_task(nom_projet: str | None = None, **context) -> list[str]:
    """Task to fetch file source configurations at runtime."""
    projet_repository = PostgresProjetRepository()
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)
    sources = projet_repository.get_list_source_fichier(nom_projet=nom_projet)
    return sources


@task()
def show_selecteur_config(config: Mapping[str, Any]) -> None:
    """Task to display selecteur configuration."""
    print(config)


@task()
def get_selecteur_config(
    nom_projet: str | None = None, storage_options: Mapping[str, SelecteurStorageOptions] | None = None, **context
) -> list[dict[str, Any]]:
    """Task to fetch the project selecteur configurations."""
    projet_repository = PostgresProjetRepository()
    nom_projet = _check_nom_projet(nom_projet=nom_projet, context=context)

    selecteurs = projet_repository.get_list_selecteur_storage_info(nom_projet=nom_projet)
    merged_config = projet_repository.merge_selecteur_config(storage_info=selecteurs, storage_options=storage_options)

    configs = [asdict(obj=sel_config, dict_factory=custom_asdict_factory) for sel_config in merged_config]
    print(type(configs))

    return configs


@task_group()
def config_projet_group(
    nom_projet: str, storage_options: Mapping[str, SelecteurStorageOptions] | None = None, **context
) -> None:
    """
    Groupe de tâches pour récupérer la configuration du projet

    Args:
        nom_projet (Optionnel): Le nom du projet

    Returns:
        None
    """
    return chain(
        [
            get_documentation_task(nom_projet=nom_projet, context=context),
            get_contact_task(nom_projet=nom_projet, context=context),
            get_source_fichier_task(nom_projet=nom_projet, context=context),
            get_selecteur_config(
                nom_projet=nom_projet,
                storage_options=storage_options,
            ),
        ]
    )
