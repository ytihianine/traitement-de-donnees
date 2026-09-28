import gzip
import logging
import os
import subprocess
import tempfile

from airflow.sdk import task
from modules.containers import DEFAULT_DAG_REPO
from modules.domain.dag.repository import DagRepository
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@task
def export_database(db_name: str, conn_id: str, dag_repo: DagRepository = DEFAULT_DAG_REPO, **context):
    db_handler = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(connection_id=conn_id),
    )
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(),
    )
    conn = db_handler.connection
    logging.info(msg=f"{conn}")

    # Environment variable for password - to avoid password prompt
    env = os.environ.copy()
    env["PGPASSWORD"] = conn["password"]

    # Export database and load it to MinIO
    logging.info(msg=f"Executing dump for database: {db_name}")
    with tempfile.NamedTemporaryFile(suffix=".sql.gz") as tmp:
        with gzip.open(tmp.name, "wb", compresslevel=9) as gz:
            proc = subprocess.Popen(
                [
                    "pg_dump",
                    "--host",
                    conn["host"],
                    "--port",
                    str(conn["port"]),
                    "--username",
                    conn["login"],
                    "--format=plain",
                    "--no-owner",
                    "--no-privileges",
                    db_name,
                ],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            for chunk in iter(
                lambda: proc.stdout.read(1024 * 1024), b""  # pyright: ignore[reportOptionalMemberAccess]
            ):
                gz.write(data=chunk)

        tmp.flush()
        execution_date = dag_repo.get_execution_date(context=context)
        curr_day = execution_date.strftime(format="%Y%m%d")
        curr_time = execution_date.strftime(format="%Hh%M")
        dest_key = f"infrastructure/sauvegarde/databases/{curr_day}/{curr_time}/{db_name}.sql.gz"
        with open(file=tmp.name, mode="rb") as f:
            s3_handler.write(
                file_path=dest_key,
                content=f.read(),
            )
        logging.info(msg=f"Successfully dumped {db_name} to S3 with key {dest_key}")
