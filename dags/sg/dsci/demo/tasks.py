from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.demo import config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.common_tasks.grist import generic_grist_processing
from modules.infra.airflow.task import create_task


def _grist_pipeline(dataset_name: str, custom_fn, **grist_kwargs) -> PipelineDescriptor:
    return PipelineDescriptor(
        input_datasets=(Dataset(dataset_name),),
        output_dataset=Dataset(dataset_name),
        operation=partial(
            generic_grist_processing,
            custom_fn=custom_fn,
            **grist_kwargs,
        ),
    )


@task_group
def referentiels() -> None:
    ref_direction = create_task(
        pipeline=_grist_pipeline(
            "ref_direction",
            process.process_ref_direction,
            txt_columns=["direction"],
        ),
        execution_options=config.execution_options,
    )
    ref_intervention = create_task(
        pipeline=_grist_pipeline(
            "ref_intervention",
            process.process_ref_intervention,
            txt_columns=["typologie_d_intervention2"],
        ),
        execution_options=config.execution_options,
    )

    # Ordre des tâches
    chain(
        [
            ref_direction(),
            ref_intervention(),
        ]
    )


@task_group
def activite() -> None:
    accompagnement = create_task(
        pipeline=_grist_pipeline(
            "accompagnement",
            process.process_accompagnement,
            cols_to_keep=[
                "id",
                "date_de_la_demande",
                "direction",
                "accompagnement",
                "statut",
                "type_d_intervention",
                "assigne_a",
                "niveau_de_complexite",
                "charge_estimee",
                "charge_consommee",
                "ecart_de_charge",
            ],
            cols_mapping={
                "direction": "id_direction",
                "type_d_intervention": "id_type_intervention",
                "assigne_a": "id_assignation",
            },
            txt_columns=[
                "accompagnement",
            ],
            date_columns=["date_de_la_demande"],
            num_columns=["charge_estimee", "charge_consommee", "ecart_de_charge"],
            ref_columns=["id_direction", "id_type_intervention", "id_assignation"],
        ),
        execution_options=config.execution_options,
    )
    # Ordre des tâches
    chain([accompagnement()])
