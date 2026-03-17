"""Configuration task group for retrieving project configuration at runtime."""

from typing import Any, Mapping
from airflow.sdk import chain, task, task_group
from dataclasses import asdict

from _types.projet import (
    SelecteurStorageOptions,
)
from utils.config.tasks import (
    get_list_documentation,
    get_list_contact,
    get_list_selecteur_storage_info,
    get_list_selector_info,
    get_list_source_fichier,
    get_list_database_info,
    get_projet_s3_info,
    get_projet_selecteur_s3,
    merge_selecteur_config,
)


@task()
def get_documentation_task(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch project documentation at runtime."""
    docs = get_list_documentation(nom_projet=nom_projet, context=context)
    return [asdict(doc) for doc in docs]


@task()
def get_contact_task(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch project contacts at runtime."""
    contacts = get_list_contact(nom_projet=nom_projet, context=context)
    return [asdict(contact) for contact in contacts]


@task()
def get_source_fichier_task(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch file source configurations at runtime."""
    sources = get_list_source_fichier(nom_projet=nom_projet, context=context)
    return [asdict(source) for source in sources]


@task()
def get_db_info_task(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch database info at runtime."""
    db_infos = get_list_database_info(nom_projet=nom_projet, context=context)
    return [asdict(db_info) for db_info in db_infos]


@task()
def get_projet_s3_info_task(
    nom_projet: str | None = None, **context
) -> Mapping[str, Any]:
    """Task to fetch project S3 configuration at runtime."""
    projet_s3 = get_projet_s3_info(nom_projet=nom_projet, context=context)
    return asdict(projet_s3)


@task()
def get_selecteur_s3_task(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch selecteur S3 configurations at runtime."""
    s3_configs = get_projet_selecteur_s3(nom_projet=nom_projet, context=context)
    return [asdict(s3_config) for s3_config in s3_configs]


@task()
def get_config_selecteur_info(
    nom_projet: str | None = None, **context
) -> list[Mapping[str, Any]]:
    """Task to fetch selecteur S3 configurations at runtime."""
    s3_db_configs = get_list_selector_info(nom_projet=nom_projet, context=context)
    return [asdict(s3_db_conf) for s3_db_conf in s3_db_configs]


@task()
def get_selecteur_config(
    nom_projet: str | None = None,
    selecteur_mapping: Mapping[str, SelecteurStorageOptions] | None = None,
    **context
) -> list[Mapping[str, Any]]:
    """Task to fetch selecteur S3 configurations at runtime."""
    if selecteur_mapping is None:
        selecteur_mapping = {}
    selecteur_info = get_list_selecteur_storage_info(
        nom_projet=nom_projet, context=context
    )
    selecteur_config = merge_selecteur_config(
        selecteur_info=selecteur_info, options_map=selecteur_mapping
    )
    return [asdict(obj=s3_db_conf) for s3_db_conf in selecteur_config]


@task()
def show_selecteur_config(config: Mapping[str, Any]) -> None:
    """Task to display selecteur configuration."""
    print(config)


@task_group()
def selecteur_tasks(nom_projet: str, **context) -> None:
    """Group of tasks to fetch selecteur configurations."""
    selecteur_info = get_list_selecteur_storage_info(
        nom_projet=nom_projet, context=context
    )

    chain(show_selecteur_config.expand(config=selecteur_info))


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
            get_db_info_task(nom_projet=nom_projet, context=context),
            get_projet_s3_info_task(nom_projet=nom_projet, context=context),
            get_selecteur_s3_task(nom_projet=nom_projet, context=context),
            get_selecteur_config(
                nom_projet=nom_projet,
                selecteur_mapping=selecteur_mapping,
                context=context,
            ),
            selecteur_tasks(nom_projet=nom_projet, context=context),
        ]
    )
