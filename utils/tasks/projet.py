"""Configuration task group for retrieving project configuration at runtime."""

from airflow.sdk import chain, task, task_group

from types.projet import (
    Contact,
    DbInfo,
    Documentation,
    ProjetS3,
    SelecteurInfo,
    SelecteurS3,
    SourceFichier,
)
from utils.config.tasks import (
    get_projet_documentation,
    get_projet_contact,
    get_projet_selecteur_info,
    get_projet_source_fichier,
    get_projet_db_info,
    get_projet_s3,
    get_projet_selecteur_s3,
)


@task()
def get_documentation_task(
    nom_projet: str | None = None, **context
) -> list[Documentation]:
    """Task to fetch project documentation at runtime."""
    docs = get_projet_documentation(nom_projet=nom_projet, context=context)
    return docs


@task()
def get_contact_task(nom_projet: str | None = None, **context) -> list[Contact]:
    """Task to fetch project contacts at runtime."""
    contacts = get_projet_contact(nom_projet=nom_projet, context=context)
    return contacts


@task()
def get_source_fichier_task(
    nom_projet: str | None = None, **context
) -> list[SourceFichier]:
    """Task to fetch file source configurations at runtime."""
    sources = get_projet_source_fichier(nom_projet=nom_projet, context=context)
    return sources


@task()
def get_db_info_task(nom_projet: str | None = None, **context) -> list[DbInfo]:
    """Task to fetch database info at runtime."""
    db_infos = get_projet_db_info(nom_projet=nom_projet, context=context)
    return db_infos


@task()
def get_projet_s3_task(nom_projet: str | None = None, **context) -> ProjetS3:
    """Task to fetch project S3 configuration at runtime."""
    projet_s3 = get_projet_s3(nom_projet=nom_projet, context=context)
    return projet_s3


@task()
def get_selecteur_s3_task(
    nom_projet: str | None = None, **context
) -> list[SelecteurS3]:
    """Task to fetch selecteur S3 configurations at runtime."""
    s3_configs = get_projet_selecteur_s3(nom_projet=nom_projet, context=context)
    return s3_configs


@task()
def get_config_selecteur_info(
    nom_projet: str | None = None, **context
) -> list[SelecteurInfo]:
    """Task to fetch selecteur S3 configurations at runtime."""
    s3_db_configs = get_projet_selecteur_info(nom_projet=nom_projet, context=context)
    return s3_db_configs


@task_group()
def configuration(nom_projet: str | None = None, **context) -> None:
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
            get_projet_s3_task(nom_projet=nom_projet, context=context),
            get_selecteur_s3_task(nom_projet=nom_projet, context=context),
        ]
    )
