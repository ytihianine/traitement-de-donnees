from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.consommation_batiment import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task


@task_group
def convert_file_to_parquet() -> None:
    conso_mens_parquet = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_mens_source"),),
            output_dataset=Dataset("conso_mens_source"),
            operation=process.process_source_bien_info_comp,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_mens_source"],
    )

    informations_batiments_parquet = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("bien_info_complementaire"),),
            output_dataset=Dataset("bien_info_complementaire"),
            operation=process.process_source_bien_info_comp,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["bien_info_complementaire"],
    )

    chain(
        [
            conso_mens_parquet(),
            informations_batiments_parquet(),
        ]
    )


@task_group(group_id="source_files")
def source_files() -> None:
    informations_batiments = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("bien_info_complementaire"),),
            output_dataset=Dataset("bien_info_complementaire"),
            operation=process.process_source_bien_info_comp,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["bien_info_complementaire"],
    )
    conso_mensuelles = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_mens_source"),),
            output_dataset=Dataset("conso_mens"),
            operation=process.process_conso_mensuelles,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_mens"],
    )
    chain([informations_batiments(), conso_mensuelles()])


@task_group(group_id="additionnal_files")
def additionnal_files() -> None:
    unpivot_conso_mens_corrigee = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_mens"),),
            output_dataset=Dataset("conso_mens_corr_unpivot"),
            operation=process.process_unpivot_conso_mens_corrigee,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_mens_corr_unpivot"],
    )
    unpivot_conso_mens_brute = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_mens"),),
            output_dataset=Dataset("conso_mens_brute_unpivot"),
            operation=process.process_unpivot_conso_mens_brute,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_mens_brute_unpivot"],
    )
    conso_annuelle = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_mens"),),
            output_dataset=Dataset("conso_annuelle"),
            operation=process.process_conso_annuelle,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_annuelle"],
    )
    conso_annuelle_unpivot = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_mens"),),
            output_dataset=Dataset("conso_annuelle_unpivot"),
            operation=process.process_conso_annuelle_unpivot,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_annuelle_unpivot"],
    )
    conso_annuelle_unpivot_comparaison = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_annuelle_unpivot"),),
            output_dataset=Dataset("conso_annuelle_unpivot_comparaison"),
            operation=process.process_conso_annuelle_unpivot_comparaison,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_annuelle_unpivot_comparaison"],
    )
    facture_annuelle_unpivot = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_annuelle"),),
            output_dataset=Dataset("facture_annuelle_unpivot"),
            operation=process.process_facture_annuelle_unpivot,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["facture_annuelle_unpivot"],
    )
    facture_annuelle_unpivot_comparaison = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("facture_annuelle_unpivot"),),
            output_dataset=Dataset("facture_annuelle_unpivot_comparaison"),
            operation=process.process_facture_annuelle_unpivot_comparaison,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["facture_annuelle_unpivot_comparaison"],
    )
    facture_annuelle_unpivot = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_annuelle"),),
            output_dataset=Dataset("facture_annuelle_unpivot"),
            operation=process.process_facture_annuelle_unpivot,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["facture_annuelle_unpivot"],
    )
    facture_annuelle_unpivot_comparaison = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("facture_annuelle_unpivot_comparaison"),),
            output_dataset=Dataset("strategie"),
            operation=process.process_facture_annuelle_unpivot_comparaison,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["facture_annuelle_unpivot_comparaison"],
    )
    conso_statut_par_fluide = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_annuelle"),),
            output_dataset=Dataset("conso_statut_par_fluide"),
            operation=process.process_conso_statut_par_fluide,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_statut_par_fluide"],
    )
    conso_statut_batiment = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("conso_statut_par_fluide"),),
            output_dataset=Dataset("conso_statut_batiment"),
            operation=process.process_conso_statut_batiment,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["conso_statut_batiment"],
    )

    chain(
        [
            unpivot_conso_mens_corrigee(),
            unpivot_conso_mens_brute(),
            conso_annuelle(),
            conso_annuelle_unpivot(),
        ],
        conso_annuelle_unpivot_comparaison(),
        facture_annuelle_unpivot(),
        facture_annuelle_unpivot_comparaison(),
        conso_statut_par_fluide(),
        conso_statut_batiment(),
    )
