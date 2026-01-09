from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain

from utils.tasks.validation import create_validate_params_task
from entities.dags import ALL_PARAM_PATHS, ETLStep, TaskConfig
from utils.tasks.file import create_parquet_converter_task
from utils.tasks.etl import create_task

from dags.sg.siep.mmsi.oad.caracteristiques import process


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


oad_carac_to_parquet = create_parquet_converter_task(
    selecteur="oad_carac",
    task_params={"task_id": "convert_oad_caracteristique_to_parquet"},
    process_func=process.process_oad_file,
)


@task_group
def tasks_oad_caracteristiques():
    sites = create_task(
        task_config=TaskConfig(task_id="sites"),
        output_selecteur="sites",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_sites)],
        # use_required_cols=True,
    )
    biens = create_task(
        task_config=TaskConfig(task_id="biens"),
        output_selecteur="biens",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_biens)],
        # use_required_cols=True,
    )
    gestionnaires = create_task(
        task_config=TaskConfig(task_id="gestionnaires"),
        output_selecteur="gestionnaires",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_gestionnaires)],
        # use_required_cols=True,
    )
    biens_gestionnaires = create_task(
        task_config=TaskConfig(task_id="biens_gestionnaires"),
        output_selecteur="biens_gest",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_biens_gestionnaires)],
        # use_required_cols=True,
    )
    biens_occupants = create_task(
        task_config=TaskConfig(task_id="biens_occupants"),
        output_selecteur="biens_occupants",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_biens_occupants)],
        # use_required_cols=True,
    )

    chain(
        [
            sites(),
            biens(),
            gestionnaires(),
            biens_gestionnaires(),
            biens_occupants(),
        ],
    )
