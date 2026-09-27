from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.api_operat import actions, config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group
def source() -> None:
    declarations_raw = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("declarations_raw"),),
            output_dataset=Dataset("declarations_raw"),
            operation=actions.liste_declaration,
            add_metadata=False,
        ),
        execution_options=config.execution_options["declarations_raw"],
    )

    consommations_raw = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("declarations_raw"),),
            output_dataset=Dataset("consommations_raw"),
            operation=actions.consommation_by_id,
            use_input_results_as_operation_args=True,
            add_metadata=False,
        ),
        execution_options=config.execution_options["consommations_raw"],
    )

    chain(
        declarations_raw(),
        consommations_raw(),
    )


@task_group
def output() -> None:
    declaration_ademe = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("declarations_raw"),),
            output_dataset=Dataset("declaration_ademe"),
            operation=process.process_declarations,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["declaration_ademe"],
    )
    activite = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("consommations_raw"),),
            output_dataset=Dataset("activite"),
            operation=process.process_detail_conso_activite,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["activite"],
    )
    indicateur = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("consommations_raw"),),
            output_dataset=Dataset("indicateur"),
            operation=process.process_detail_conso_indicateur,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["indicateur"],
    )
    detail = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("consommations_raw"),),
            output_dataset=Dataset("detail"),
            operation=process.process_detail_conso,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["detail"],
    )
    chain(
        declaration_ademe(),
        activite(),
        indicateur(),
        detail(),
    )
