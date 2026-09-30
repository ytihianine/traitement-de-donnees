from dags.sg.siep.mmsi.oad_referentiel import process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import create_task

from dags.applications.configuration_projets import config

ref_typologie = create_task(
    pipeline=PipelineDescriptor(
        input_datasets=(Dataset("ref_typologie"),),
        output_dataset=Dataset("ref_typologie"),
        operation=process.process_ref_typologie,
        add_metadata=True,
    ),
    execution_options=config.execution_options,
)
