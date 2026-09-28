import logging
from dataclasses import dataclass

import pandas as pd

from modules.domain.dataset.ports import DatasetLocationProviderFactory
from modules.domain.dataset.repository import DatasetContextRepository
from modules.domain.pipeline.model import ExecutionOptions, PipelineDescriptor
from modules.domain.pipeline.output import OutputAdapterRegistry
from modules.domain.projet.model import ProjetMetadata
from modules.domain.projet.repository import ProjetRepository
from modules.logs import df_info


def _add_metadata(df: pd.DataFrame, metadata: ProjetMetadata) -> pd.DataFrame:

    df["import_timestamp"] = metadata.import_timestamp
    df["snapshot_id"] = str(metadata.snapshot_id)
    df["snapshot_id_parent"] = str(metadata.snapshot_id_parent) if metadata.snapshot_id_parent is not None else None

    return df


@dataclass(frozen=True)
class PipelineRunner:
    projet_repo: ProjetRepository
    dataset_context_repo: DatasetContextRepository
    output_adapter_registry: OutputAdapterRegistry
    location_provider_factory: DatasetLocationProviderFactory

    def run(
        self,
        nom_projet: str,
        pipeline: PipelineDescriptor,
        execution_options: ExecutionOptions,
    ):

        # ===============================
        # Read data
        # ===============================
        input_data = {}
        for dataset in pipeline.input_datasets:
            logging.info(msg=f"▶ Reading dataset: {dataset.name}")
            dataset_context = self.dataset_context_repo.get(nom_projet=nom_projet, nom_dataset=dataset.name)
            if pipeline.use_input_results_as_operation_args:
                dataset_location = dataset_context.tmp_location
            else:
                dataset_location = dataset_context.src_location

            reader = self.location_provider_factory.create(dataset_location=dataset_location)
            df = reader.read(location=dataset_location.validate_location)
            input_data[f"df_{dataset.name}"] = df

        if len(input_data) == 1:
            input_data = {"df": next(iter(input_data.values()))}

        # ===============================
        # Execution operation data
        # ===============================
        logging.info(msg=f"Running pipeline operation: {pipeline.operation.__name__}")
        result = pipeline.operation(**input_data)

        if result is None:
            logging.warning(msg="Pipeline operation returned None. Ending pipeline execution.")
            return

        if pipeline.add_metadata:
            if not isinstance(result, pd.DataFrame):
                raise TypeError("add_metadata is only supported for DataFrame results")
            projet_metadata = self.projet_repo.get_projet_metadata(nom_projet=nom_projet)
            result = _add_metadata(df=result, metadata=projet_metadata)

        if isinstance(result, pd.DataFrame):
            df_info(df=result, df_name=f"{pipeline.output_dataset.name} - df to export")

        # ===============================
        # Export data
        # ===============================
        output_dataset_context = self.dataset_context_repo.get(
            nom_projet=nom_projet, nom_dataset=pipeline.output_dataset.name
        )
        output_location = output_dataset_context.dest_location
        provider = self.location_provider_factory.create(dataset_location=output_location)
        adapter = self.output_adapter_registry.get_adapter(result)
        logging.info(
            msg=(
                f"Exporting pipeline result of type {type(result).__name__} "
                f"using {type(adapter).__name__} to {output_location.validate_location}"
            )
        )
        adapter.write(
            output=result,
            provider=provider,
            location=output_location.validate_location,
        )
