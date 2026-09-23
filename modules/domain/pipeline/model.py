from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from modules.domain.dataset.model import Dataset


@dataclass(frozen=True)
class PipelineDescriptor(ABC):
    input_datasets: tuple[Dataset]
    output_dataset: Dataset
    transformations: tuple[Callable[..., pd.DataFrame]]
    add_metadata: bool = True
    selecteur_config_task_id: str = "get_selecteur_config"
