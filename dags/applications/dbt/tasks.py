import os
import subprocess
import sys
from pathlib import Path

from airflow.sdk import task
from modules.constants import DEFAULT_PG_DATA_CONN_ID, get_root_folder
from modules.containers import DEFAULT_DAG_REPO
from modules.domain.dag.model import FeatureFlags
from modules.domain.dag.repository import DagRepository
from modules.infra.airflow.dag import should_skip_task
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler


def run_dbt_build(
    profile_dir: Path, project_dir: Path, db_host: str, db_name: str, db_schema: str, db_user: str, db_password: str
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
    result = subprocess.run(
        args=["dbt", "build", "--profiles-dir", str(profile_dir), "--project-dir", str(project_dir)],
        capture_output=True,
        check=False,
        env=_env,
        text=True,
    )
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)


profile_dir = Path(__file__).parent
projet_repo = get_root_folder()
config_projet_path = Path(projet_repo) / "dags" / "applications" / "configuration_projets" / "dbt"


@task(task_id="build_dimension_models")
def run_dimension_models(
    project_dir: Path,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    dag_repo: DagRepository = DEFAULT_DAG_REPO,
    **context,
) -> None:
    """Build the configuration project dimension models with dbt."""
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

    profile_dir = Path(get_root_folder()) / "dags" / "applications" / "dbt"

    run_dbt_build(
        profile_dir=profile_dir,
        project_dir=project_dir,
        db_host=str(required_connection_values["host"]),
        db_name=str(required_connection_values["dbname"]),
        db_schema=db_info.prod_schema,
        db_user=str(required_connection_values["user"]),
        db_password=str(connection.get("password") or ""),
    )
