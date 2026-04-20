import os
from pathlib import Path
import subprocess

from airflow.sdk import Variable

from src.infradatabase.factory import create_db_handler
from src.infrafile_system.factory import create_file_handler
from src.utils.config.dag_params import get_project_name
from src.utils.config.tasks import get_list_selecteur_storage_info
from src._enums.filesystem import FileHandlerType
from src.constants import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID


def create_dump_files(context: dict) -> None:
    """
    Perform a PostgreSQL pg_dumpall command and store the result in a local file.
    """
    # config
    nom_projet = get_project_name(context=context)
    selecteur_storage_info = get_list_selecteur_storage_info(nom_projet=nom_projet)

    # Variables
    db_handler = create_db_handler(connection_id="db_data_store")

    # Hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        connection_id=DEFAULT_S3_CONN_ID,
        bucket=DEFAULT_S3_BUCKET,
    )
    local_handler = create_file_handler(handler_type=FileHandlerType.LOCAL)
    conn = db_handler.get_uri()

    split_conn_dsn = conn.split(sep="://")[1].split(sep="/")[0].split(sep="@")
    print(split_conn_dsn)
    credentials = split_conn_dsn[0].split(sep=":")
    username = credentials[0]
    connexion = split_conn_dsn[1].split(sep=":")
    host = connexion[0]
    port = connexion[1]

    # Environment variable for password - to avoid password prompt
    env = os.environ.copy()
    env["PGPASSWORD"] = Variable.get(key="db_main_password")

    for config in selecteur_storage_info:
        local_path = Path("/tmp") / config.filename
        db_name = config.id_source
        dest_tmp_key = config.get_full_s3_key(with_tmp_segment=True)

        # Construct pg_dump command (without file output)
        command = [
            "pg_dump",
            f"--host={host}",
            f"--port={port}",
            f"--username={username}",
            "-Fc",  # Custom format
            "--no-owner",
            "-d",
            db_name,
        ]

        print(f"Executing dump for database: {db_name}")

        # Local + S3 dump
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        ) as proc:
            # Capture output and wait for process to finish
            stdout, stderr = proc.communicate()

            if proc.returncode != 0:
                raise ValueError(
                    f"Error dumping {db_name}: {stderr.decode().strip() or 'Unknown error'}"
                )

            # --- 1. Write dump locally (atomic write via file_handler) ---
            local_handler.write(file_path=local_path, content=stdout)

            # --- 2. Upload local dump to S3 ---
            with open(file=local_path, mode="rb") as f:
                s3_handler.write(file_path=dest_tmp_key, content=f)

            print(
                f"Successfully dumped {db_name} to local: {local_path}, and uploaded to S3: {dest_tmp_key}"  # noqa
            )

            local_handler.delete(file_path=local_path)
