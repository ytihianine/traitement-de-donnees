from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.oad import config
from dags.sg.siep.mmsi.oad.caracteristiques import process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task

oad_carac_to_parquet = create_task(
    pipeline=PipelineDescriptor(
        input_datasets=(Dataset("oad_carac"),),
        output_dataset=Dataset("oad_carac"),
        operation=process.process_oad_file,
        add_metadata=False,
    ),
    execution_options=config.execution_options,
)


@task_group
def tasks_oad_caracteristiques():
    sites = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_carac"),),
            output_dataset=Dataset("sites"),
            operation=process.process_sites,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    biens = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_carac"),),
            output_dataset=Dataset("biens"),
            operation=process.process_biens,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    gestionnaires = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_carac"),),
            output_dataset=Dataset("gestionnaires"),
            operation=process.process_gestionnaires,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    biens_gestionnaires = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_carac"),),
            output_dataset=Dataset("biens_gest"),
            operation=process.process_biens_gestionnaires,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    biens_occupants = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_carac"),),
            output_dataset=Dataset("biens_occupants"),
            operation=process.process_biens_occupants,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
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
