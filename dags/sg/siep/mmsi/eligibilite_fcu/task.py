from dags.sg.siep.mmsi.eligibilite_fcu import actions, config, process
from modules.domain.dataset.model import Dataset
from modules.domain.pipeline.model import PipelineDescriptor
from modules.infra.airflow.task import (
    create_task,
)

bien_localisation = create_task(
    pipeline=PipelineDescriptor(
        input_datasets=(Dataset("bien_localisation"),),
        output_dataset=Dataset("bien_localisation"),
        operation=actions.eligibilite_fcu,
        add_metadata=True,
    ),
    execution_options=config.execution_options,
)


process_fcu_result = create_task(
    pipeline=PipelineDescriptor(
        input_datasets=(Dataset("bien_localisation"),),
        output_dataset=Dataset("fcu_result"),
        operation=process.process_result,
        add_metadata=True,
    ),
    execution_options=config.execution_options,
)
