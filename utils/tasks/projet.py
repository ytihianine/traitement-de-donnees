"""Configuration task group for retrieving project configuration at runtime."""

from typing import Any, Mapping
from airflow.sdk import chain, task, task_group
from dataclasses import asdict

from _types.projet import (
    SelecteurStorageOptions,
)
from utils.config.dag_params import get_project_name
from utils.config.tasks import (
    get_list_documentation,
    get_list_contact,
    get_list_selecteur_storage_info,
    get_list_source_fichier,
    merge_selecteur_config,
)


@task()
def get_documentation_task(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch project documentation at runtime."""
    if nom_projet is None:
        nom_projet = get_project_name(context=context)
    docs = get_list_documentation(nom_projet=nom_projet)
    return [asdict(obj=doc) for doc in docs]


@task()
def get_contact_task(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch project contacts at runtime."""
    if nom_projet is None:
        nom_projet = get_project_name(context=context)
    contacts = get_list_contact(nom_projet=nom_projet)
    return [asdict(obj=contact) for contact in contacts]


@task()
def get_source_fichier_task(nom_projet: str | None = None, **context) -> list[str]:
    """Task to fetch file source configurations at runtime."""
    if nom_projet is None:
        nom_projet = get_project_name(context=context)
    sources = get_list_source_fichier(nom_projet=nom_projet)
    return sources


@task()
def show_selecteur_config(config: Mapping[str, Any]) -> None:
    """Task to display selecteur configuration."""
    print(config)


@task_group()
def get_selecteur_config(
    nom_projet: str,
    selecteur_options: Mapping[str, SelecteurStorageOptions] | None = None,
) -> None:
    """Group of tasks to fetch selecteur configurations."""
    selecteur_info = get_list_selecteur_storage_info(nom_projet=nom_projet)
    selecteur_config = merge_selecteur_config(
        selecteur_info=selecteur_info, options_map=selecteur_options
    )

    chain(show_selecteur_config.expand(config=selecteur_config))


@task_group()
def config_projet_group(
    nom_projet: str,
    selecteur_mapping: Mapping[str, SelecteurStorageOptions] | None = None,
    **context
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
                selecteur_options=selecteur_mapping,
            ),
        ]
    )
