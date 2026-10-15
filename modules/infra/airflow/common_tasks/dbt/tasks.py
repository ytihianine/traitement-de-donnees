import logging
import os
import subprocess
from pathlib import Path

from airflow.sdk import task
from modules.constants import DEFAULT_PG_DATA_CONN_ID, get_root_folder
from modules.containers import DEFAULT_DAG_REPO
from modules.domain.dag.model import FeatureFlags
from modules.domain.dag.repository import DagRepository
from modules.infra.airflow.dag import should_skip_task
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler


def create_dbt_logs(logs_path: Path) -> None:
    logging.info(msg=f"Creating logs directory and file @ {logs_path}")

    # Create dir if not exists
    dir_path = logs_path.parent
    logging.info(msg=f"Creating logs directory @ {dir_path}")
    os.mkdir(path=dir_path)

    # Create file if not exists
    logging.info(msg="Creating logs file")
    with open(file=logs_path, mode="w"):
        pass


def delete_dbt_logs(logs_path: Path) -> None:
    logging.info(msg=f"Deleting logs file @ {logs_path}")
    # Delete log file if exists
    if os.path.exists(path=logs_path):
        os.remove(path=logs_path)
    else:
        logging.info(msg="File does not exist ... Skipping")


def read_dbt_logs(logs_path: Path) -> None:
    logging.info(msg=f"Reading dbt logs @ {logs_path}")

    with open(file=logs_path) as file:
        # Read the content of the file
        file_content = file.read()

        # Print the content
        logging.info(msg=f"{file_content}")


def run_dbt_build(
    profile_dir: Path,
    project_dir: Path,
    logs_path: Path,
    db_host: str,
    db_name: str,
    db_schema: str,
    db_user: str,
    db_password: str,
) -> None:
    _env = os.environ.copy()
    _env.update(
        {
            "DBT_DB_HOST": db_host,
            "DBT_DB_NAME": db_name,
            "DBT_DB_SCHEMA": db_schema,
            "DBT_DB_USER": db_user,
            "DBT_DB_PASSWORD": db_password,
        }
    )
    logs_dir_path = logs_path.parent
    cmd = [
        "dbt",
        "build",
        "--profiles-dir",
        str(profile_dir),
        "--project-dir",
        str(project_dir),
        "--log-path",
        str(logs_dir_path),
        "--debug",
    ]

    logging.info(msg=f"Executing dbt command: {cmd}")
    logging.info(msg=f"dbt project directory: {project_dir}")
    logging.info(msg=f"dbt profiles directory: {profile_dir}")

    result = subprocess.run(
        args=cmd,
        check=False,
        env=_env,
        text=True,
        cwd=str(project_dir),
    )
    if result.stdout:
        logging.info(msg=result.stdout)
    if result.stderr:
        logging.error(msg=result.stderr)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            returncode=result.returncode,
            cmd=result.args,
            output=result.stdout,
            stderr=result.stderr,
        )


@task(task_id="build_dimension_models")
def dbt_build_models(
    project_dir_subpath: Path,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    **context,
) -> None:
    """
    Build the configuration project dimension models with dbt.

    Args:
        project_dir_subpath (Path): The sub-path to the dbt project directory.
        pg_conn_id (str): The Postgres connection ID.
        dag_repo (DagRepository): The DAG repository instance.
        **context: Additional context passed by Airflow.
    """
    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    db_info = dag_repo.get_db_info(context=context)
    connection = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=pg_conn_id),
    ).connection
    required_connection_values = {
        "host": connection.get("host"),
        "dbname": connection.get("schema"),
        "user": connection.get("login"),
    }
    missing_connection_values = [key for key, value in required_connection_values.items() if not value]
    if missing_connection_values:
        raise ValueError(f"Missing database connection values required by dbt: {', '.join(missing_connection_values)}")

    repo_dir = Path(get_root_folder())
    profile_dir = Path(get_root_folder()) / "modules" / "infra" / "airflow" / "common_tasks" / "dbt"
    logs_path = repo_dir / project_dir_subpath / "dbt_logs.txt"

    create_dbt_logs(logs_path=logs_path)
    run_dbt_build(
        profile_dir=profile_dir,
        project_dir=repo_dir / project_dir_subpath,
        logs_path=logs_path,
        db_host=str(required_connection_values["host"]),
        db_name=str(required_connection_values["dbname"]),
        db_schema=db_info.prod_schema,
        db_user=str(required_connection_values["user"]),
        db_password=str(connection.get("password") or ""),
    )
    read_dbt_logs(logs_path=logs_path)
    delete_dbt_logs(logs_path=logs_path)
