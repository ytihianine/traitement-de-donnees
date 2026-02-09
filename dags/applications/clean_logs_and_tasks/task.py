import logging
from airflow.sdk import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.config.vars import DEFAULT_S3_CONN_ID


@task(task_id="clean_s3")
def clean_s3() -> None:
    from datetime import datetime, timedelta

    s3_handler = S3Hook(aws_conn_id=DEFAULT_S3_CONN_ID, bucket="dsci")
    days_to_keep = 60

    date_to_clean_before = datetime.now() - timedelta(days=days_to_keep)

    s3_keys = s3_handler.list_keys(to_datetime=date_to_clean_before)
    logging.info(msg=s3_keys)


@task(task_id="clean_old_logs")
def clean_old_logs() -> None:
    import subprocess
    from datetime import datetime, timedelta

    days_to_keep = 60
    date_to_clean_before = datetime.now() - timedelta(days=days_to_keep)

    # Construct pg_dumpall command with Kubernetes service DNS
    command = [
        f"airflow db clean --clean-before-timestamp '{date_to_clean_before}' --verbose"
    ]

    logging.info(msg=command)

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    # Check for errors
    if result.returncode != 0:
        raise ValueError(f"Error occurred: {result.stderr}")
    else:
        logging.info(msg="Command executed successfully. Metadatadb has been cleared")


@task(task_id="clean_skipped_logs")
def clean_skipped_logs() -> None:
    pass
