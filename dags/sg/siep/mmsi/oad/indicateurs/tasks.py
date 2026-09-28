from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.oad import config
from dags.sg.siep.mmsi.oad.indicateurs import process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task

oad_indic_to_parquet = create_task(
    pipeline=PipelineDescriptor(
        input_datasets=(Dataset("oad_indic"),),
        output_dataset=Dataset("oad_indic"),
        operation=process.process_oad_indic,
        use_input_results_as_operation_args=False,
        add_metadata=True,
    ),
    execution_options=config.execution_options["oad_indic"],
)


@task_group
def tasks_oad_indicateurs():
    oad_indic = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset("oad_indic"),
                Dataset("biens"),
            ),
            output_dataset=Dataset("oad_indic"),
            operation=process.filtrer_oad_indic,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["oad_indic"],
    )
    accessibilite = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("accessibilite"),
            operation=process.process_accessibilite,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["accessibilite"],
    )
    accessibilite_detail = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("accessibilite_detail"),
            operation=process.process_accessibilite_detail,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["accessibilite_detail"],
    )
    bacs = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("bacs"),
            operation=process.process_bacs,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["bacs"],
    )
    bails = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("bails"),
            operation=process.process_bails,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["bails"],
    )
    couts = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("couts"),
            operation=process.process_couts,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["couts"],
    )
    deet_energie_ges = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("deet_energie_ges"),
            operation=process.process_deet_energie,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["deet_energie_ges"],
    )
    etat_de_sante = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("etat_de_sante"),
            operation=process.process_eds,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["etat_de_sante"],
    )
    exploitation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("exploitation"),
            operation=process.process_exploitation,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["exploitation"],
    )
    note = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("note"),
            operation=process.process_notes,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["note"],
    )
    effectif = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("effectif"),
            operation=process.process_effectif,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["effectif"],
    )
    proprietaire = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("proprietaire"),
            operation=process.process_proprietaire,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["proprietaire"],
    )
    reglementation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("reglementation"),
            operation=process.process_reglementation,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["reglementation"],
    )
    surface = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("surface"),
            operation=process.process_surface,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["surface"],
    )
    typologie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("typologie"),
            operation=process.process_typologie,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["typologie"],
    )
    valeur = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(Dataset("oad_indic"),),
            output_dataset=Dataset("valeur"),
            operation=process.process_valeur,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["valeur"],
    )
    localisation = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset("oad_carac"),
                Dataset("oad_indic"),
                Dataset("biens"),
            ),
            output_dataset=Dataset("localisation"),
            operation=process.process_localisation,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["localisation"],
    )
    strategie = create_task(
        pipeline=PipelineDescriptor(
            input_datasets=(
                Dataset("oad_carac"),
                Dataset("oad_indic"),
                Dataset("biens"),
            ),
            output_dataset=Dataset("strategie"),
            operation=process.process_strategie,
            use_input_results_as_operation_args=True,
            add_metadata=True,
        ),
        execution_options=config.execution_options["strategie"],
    )

    chain(
        oad_indic(),
        [
            accessibilite(),
            accessibilite_detail(),
            bacs(),
            bails(),
            couts(),
            deet_energie_ges(),
            etat_de_sante(),
            exploitation(),
            localisation(),
            note(),
            effectif(),
            proprietaire(),
            reglementation(),
            strategie(),
            surface(),
            typologie(),
            valeur(),
        ],
    )
