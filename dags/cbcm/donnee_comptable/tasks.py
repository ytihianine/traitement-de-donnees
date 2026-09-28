from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.cbcm.donnee_comptable import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group(group_id="source_files")
def source_files() -> None:
    demande_achat = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_achat"),),
            output_dataset=Dataset("demande_achat"),
            operation=process.process_demande_achat,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    engagement_juridique = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("engagement_juridique"),),
            output_dataset=Dataset("engagement_juridique"),
            operation=process.process_engagement_juridique,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    demande_paiement = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_paiement"),),
            output_dataset=Dataset("demande_paiement"),
            operation=process.process_demande_paiement,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    demande_paiement_flux = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_paiement_flux"),),
            output_dataset=Dataset("demande_paiement_flux"),
            operation=process.process_demande_paiement_flux,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    demande_paiement_sfp = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_paiement_sfp"),),
            output_dataset=Dataset("demande_paiement_sfp"),
            operation=process.process_demande_paiement_sfp,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    demande_paiement_carte_achat = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_paiement_carte_achat"),),
            output_dataset=Dataset("demande_paiement_carte_achat"),
            operation=process.process_demande_paiement_carte_achat,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    demande_paiement_journal_pieces = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("demande_paiement_journal_pieces"),),
            output_dataset=Dataset("demande_paiement_journal_pieces"),
            operation=process.process_demande_paiement_journal_pieces,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    delai_global_paiement = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("delai_global_paiement"),),
            output_dataset=Dataset("delai_global_paiement"),
            operation=process.process_delai_global_paiement,
            add_metadata=True,
        ),
        execution_options=config.execution_options,
    )
    chain(
        [
            demande_achat(),
            engagement_juridique(),
            demande_paiement(),
            demande_paiement_flux(),
            demande_paiement_sfp(),
            demande_paiement_carte_achat(),
            demande_paiement_journal_pieces(),
            delai_global_paiement(),
        ]
    )


@task_group(group_id="dataset_additionnel")
def datasets_additionnels() -> None:
    demande_paiement_complet = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset(name="demande_paiement"),
                Dataset(name="demande_paiement_carte_achat"),
                Dataset(name="demande_paiement_flux"),
                Dataset(name="demande_paiement_journal_pieces"),
                Dataset(name="demande_paiement_sfp"),
            ),
            output_dataset=Dataset("demande_paiement_complet"),
            use_input_results_as_operation_args=True,
            operation=process.process_demande_paiement_complet,
        ),
        execution_options=config.execution_options,
    )

    chain(demande_paiement_complet())
