"""Exécuter un projet dbt sur la base de données configurée dans scripts/.env."""

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml
from modules.constants import custom_logger, get_root_folder
from modules.infra.airflow.common_tasks.dbt.tasks import run_dbt_build

PROFILES_SUBPATH = Path("modules") / "infra" / "airflow" / "common_tasks" / "dbt"


@dataclass(frozen=True)
class DBTInfo:
    projet_dir: str
    projet_schema: str


@dataclass(frozen=True)
class DBInfo:
    host: str
    port: str
    name: str
    user: str
    password: str


@dataclass(frozen=True)
class Config:
    db: DBInfo
    dbt: DBTInfo


def main() -> None:
    dir = os.path.dirname(os.path.realpath(__file__))
    config_path = Path(dir, "config.yaml")

    # Load config
    with open(file=config_path) as f:
        _config = yaml.safe_load(stream=f)
        config = Config(
            db=_config.get("db"),
            dbt=_config.get("dbt"),
        )

    projet_schema = config.dbt.projet_schema
    project_subpath = config.dbt.projet_dir
    repo_dir = Path(get_root_folder())
    project_dir = repo_dir / project_subpath
    profiles_dir = repo_dir / PROFILES_SUBPATH
    tmp_dir = Path("/tmp") / project_subpath

    if not project_dir.is_dir():
        custom_logger.error(msg=f"Projet dbt introuvable: {project_dir}")
        sys.exit(1)
    if not (profiles_dir / "profiles.yml").is_file():
        custom_logger.error(msg=f"Profils dbt introuvables: {profiles_dir / 'profiles.yml'}")
        sys.exit(1)

    custom_logger.info(msg=f"Projet dbt: {project_dir}")
    custom_logger.info(
        msg=f"Base de données: {config.db.user}@{config.db.host}:{config.db.port}/{config.db.name} (schema: {projet_schema})"
    )

    try:
        # run_dbt_build appelle `dbt` présent dans le binaire courant
        run_dbt_build(
            profile_dir=profiles_dir,
            project_dir=project_dir,
            tmp_dir=tmp_dir,
            db_host=config.db.host,
            db_name=config.db.name,
            db_schema=projet_schema,
            db_user=config.db.user,
            db_password=config.db.password,
        )
    except subprocess.CalledProcessError as exc:
        custom_logger.error(msg=f"dbt build a échoué (code {exc.returncode})")
        sys.exit(exc.returncode)
    custom_logger.info(msg="dbt build terminé avec succès")


if __name__ == "__main__":
    main()
