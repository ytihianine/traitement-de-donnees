from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.cgefi.barometre import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task

SELECTEUR_BAROMETRE = "barometre"
SELECTEUR_ORGA_MERGE = "organisme_merge"


@task_group()
def source_files() -> None:
    cartographie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("cartographie"),),
            output_dataset=Dataset("cartographie"),
            operation=process.process_cartographie,
            use_input_results_as_operation_args=False,
            add_metadata=True,
        ),
        execution_options=config.execution_options["cartographie"],
    )
    efc = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("efc"),),
            output_dataset=Dataset("efc"),
            operation=process.process_efc,
            use_input_results_as_operation_args=False,
            add_metadata=True,
        ),
        execution_options=config.execution_options["efc"],
    )
    recommandation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("recommandation"),),
            output_dataset=Dataset("recommandation"),
            operation=process.process_recommandation,
            use_input_results_as_operation_args=False,
            add_metadata=True,
        ),
        execution_options=config.execution_options["recommandation"],
    )
    fiche_signaletique = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("fiche_signaletique"),),
            output_dataset=Dataset("fiche_signaletique"),
            operation=process.process_fiche_signaletique,
            use_input_results_as_operation_args=False,
            add_metadata=True,
        ),
        execution_options=config.execution_options["fiche_signaletique"],
    )
    rapport_annuel = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("rapport_annuel"),),
            output_dataset=Dataset("rapport_annuel"),
            operation=process.process_rapport_annuel,
            use_input_results_as_operation_args=False,
            add_metadata=True,
        ),
        execution_options=config.execution_options["rapport_annuel"],
    )
    organisme = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("organisme"),),
            output_dataset=Dataset("organisme"),
            operation=process.process_organisme,
            use_input_results_as_operation_args=False,
            add_metadata=True,
        ),
        execution_options=config.execution_options["organisme"],
    )
    organisme_hc = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("organisme_hors_corpus"),),
            output_dataset=Dataset("organisme_hors_corpus"),
            operation=process.process_organisme_hors_corpus,
            use_input_results_as_operation_args=False,
            add_metadata=True,
        ),
        execution_options=config.execution_options["organisme_hors_corpus"],
    )

    # ordre des tâches
    chain(
        [
            cartographie(),
            efc(),
            recommandation(),
            fiche_signaletique(),
            rapport_annuel(),
            organisme(),
            organisme_hc(),
        ]
    )
