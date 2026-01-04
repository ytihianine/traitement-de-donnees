from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.config.types import ALL_PARAM_PATHS, ETLStep, TaskConfig
from utils.tasks.etl import (
    create_task,
)

from dags.applications.catalogue.update import actions, process
from utils.tasks.validation import create_validate_params_task


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group()
def source_database() -> None:
    pg_info_scan = create_task(
        task_config=TaskConfig(task_id="pg_info_scan"),
        output_selecteur="pg_info_scan",
        steps=[
            ETLStep(
                fn=actions.pg_info_scan,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    pg_info_extract_catalogue = create_task(
        task_config=TaskConfig(task_id="pg_info_extract_catalogue"),
        output_selecteur="pg_info_extract_catalogue",
        input_selecteurs=["pg_info_scan"],
        steps=[
            ETLStep(
                fn=process.pg_info_extract_catalogue,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    pg_info_extract_dictionnaire = create_task(
        task_config=TaskConfig(task_id="pg_info_extract_dictionnaire"),
        output_selecteur="pg_info_extract_dictionnaire",
        input_selecteurs=["pg_info_scan"],
        steps=[
            ETLStep(
                fn=process.pg_info_extract_dictionnaire,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )

    """ Tasks order """
    chain(
        pg_info_scan(),
        [
            pg_info_extract_catalogue(),
            pg_info_extract_dictionnaire(),
        ],
    )


@task_group()
def update_grist_catalogue() -> None:
    # Catalogue
    get_catalogue = create_task(
        task_config=TaskConfig(task_id="get_catalogue"),
        output_selecteur="get_catalogue",
        steps=[
            ETLStep(
                fn=actions.get_catalogue,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    compare_catalogue = create_task(
        task_config=TaskConfig(task_id="compare_catalogue"),
        output_selecteur="compare_catalogue",
        input_selecteurs=["pg_info_extract_catalogue", "get_catalogue"],
        steps=[
            ETLStep(
                fn=process.compare_catalogue,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    process_catalogue = create_task(
        task_config=TaskConfig(task_id="process_catalogue"),
        output_selecteur="process_catalogue",
        input_selecteurs=["compare_catalogue"],
        steps=[
            ETLStep(
                fn=process.process_catalogue,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    load_catalogue = create_task(
        task_config=TaskConfig(task_id="load_catalogue"),
        output_selecteur="load_catalogue",
        input_selecteurs=["process_catalogue"],
        steps=[
            ETLStep(
                fn=actions.load_catalogue,
            )
        ],
        export_output=False,
    )

    # Dictionnaire
    get_dictionnaire = create_task(
        task_config=TaskConfig(task_id="get_dictionnaire"),
        output_selecteur="get_dictionnaire",
        steps=[
            ETLStep(
                fn=actions.get_dictionnaire,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    compare_dictionnaire = create_task(
        task_config=TaskConfig(task_id="compare_dictionnaire"),
        output_selecteur="compare_dictionnaire",
        input_selecteurs=["pg_info_extract_dictionnaire", "get_dictionnaire"],
        steps=[
            ETLStep(
                fn=process.compare_dictionnaire,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    process_dictionnaire = create_task(
        task_config=TaskConfig(task_id="process_dictionnaire"),
        output_selecteur="process_dictionnaire",
        input_selecteurs=["compare_dictionnaire"],
        steps=[
            ETLStep(
                fn=process.process_dictionnaire,
            )
        ],
        add_import_date=False,
        add_snapshot_id=False,
    )
    load_dictionnaire = create_task(
        task_config=TaskConfig(task_id="load_dictionnaire"),
        output_selecteur="load_dictionnaire",
        input_selecteurs=["process_dictionnaire"],
        steps=[
            ETLStep(
                fn=actions.load_dictionnaire,
            )
        ],
        export_output=False,
    )

    chain(
        [
            get_catalogue(),
            get_dictionnaire(),
        ],
        [
            compare_catalogue(),
            compare_dictionnaire(),
        ],
        [
            process_catalogue(),
            process_dictionnaire(),
        ],
        load_catalogue(),
        load_dictionnaire(),
    )


# @task_group()
# def update_catalogue() -> None:
#     get_catalogue = create_action_to_file_etl_task(
#         output_selecteur="get_catalogue",
#         task_id="get_catalogue",
#         action_func=actions.get_catalogue,
#         add_import_date=False,
#         add_snapshot_id=False,
#     )
#     compare_catalogue = create_multi_files_input_etl_task(
#         input_selecteurs=["pg_info_extract_catalogue", "get_catalogue"],
#         output_selecteur="compare_catalogue",
#         process_func=process.compare_catalogue,
#         add_import_date=False,
#         add_snapshot_id=False,
#     )
#     process_catalogue = create_multi_files_input_etl_task(
#         output_selecteur="process_catalogue",
#         input_selecteurs=["compare_catalogue"],
#         process_func=process.process_catalogue,
#         add_import_date=False,
#         add_snapshot_id=False,
#     )
#     load_catalogue = create_action_from_multi_input_files_etl_task(
#         task_id="load_catalogue",
#         input_selecteurs=["process_catalogue"],
#         action_func=actions.load_catalogue,
#     )
#     chain(get_catalogue(), compare_catalogue(), process_catalogue(), load_catalogue())


# @task_group()
# def update_dictionnaire() -> None:
#     get_dictionnaire = create_action_to_file_etl_task(
#         output_selecteur="get_dictionnaire",
#         task_id="get_dictionnaire",
#         action_func=actions.get_dictionnaire,
#         add_import_date=False,
#         add_snapshot_id=False,
#     )
#     compare_dictionnaire = create_multi_files_input_etl_task(
#         input_selecteurs=["pg_info_extract_dictionnaire", "get_dictionnaire"],
#         output_selecteur="compare_dictionnaire",
#         process_func=process.compare_dictionnaire,
#         add_import_date=False,
#         add_snapshot_id=False,
#     )
#     process_dictionnaire = create_multi_files_input_etl_task(
#         output_selecteur="process_dictionnaire",
#         input_selecteurs=["compare_dictionnaire"],
#         process_func=process.process_dictionnaire,
#         add_import_date=False,
#         add_snapshot_id=False,
#     )
#     load_dictionnaire = create_action_from_multi_input_files_etl_task(
#         task_id="load_dictionnaire",
#         input_selecteurs=["process_dictionnaire"],
#         action_func=actions.load_catalogue,
#     )

#     chain(
#         get_dictionnaire(),
#         compare_dictionnaire(),
#         process_dictionnaire(),
#         load_dictionnaire(),
#     )
