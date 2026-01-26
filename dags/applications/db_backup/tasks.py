from _types.dags import (
    ETLStep,
    TaskConfig,
)
from utils.tasks.etl import create_task

from dags.applications.db_backup import actions


dump_databases = create_task(
    task_config=TaskConfig(task_id="db_backup"),
    output_selecteur="db_backup",
    steps=[
        ETLStep(
            fn=actions.create_dump_files,
            use_context=True,
        ),
    ],
    export_output=False,
)
