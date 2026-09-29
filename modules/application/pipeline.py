import logging
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from modules.domain.dataset.model import Dataset, TypeLocation
from modules.domain.dataset.ports import DatasetLocationProviderFactory
from modules.domain.dataset.repository import DatasetContextRepository
from modules.domain.pipeline.model import ExecutionOptions, PipelineDescriptor
from modules.domain.pipeline.output import OutputAdapterRegistry
from modules.domain.projet.model import ProjetMetadata
from modules.domain.projet.repository import ProjetRepository
from modules.infra.file_system.dataset_location import GRIST_SQLITE_PATH_READ_OPTION
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler
from modules.logs import df_info

GRIST_DOCUMENT_DATASET_NAME = "grist_doc"
GRIST_DOCUMENT_DIRECTORY = Path("/tmp")


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

    def _download_grist_doc_locally(self, nom_projet: str) -> Path:
        """Download the project's Grist SQLite document to temporary storage."""
        grist_doc_context = self.dataset_context_repo.get(
            nom_projet=nom_projet,
            nom_dataset=GRIST_DOCUMENT_DATASET_NAME,
        )
        grist_doc_src_loc = grist_doc_context.src_loc
        if grist_doc_src_loc.type_location != TypeLocation.GRIST:
            raise ValueError(f"{GRIST_DOCUMENT_DATASET_NAME} source must be a Grist location")
        document_id = grist_doc_src_loc.validate_location

        grist_doc_tmp_loc = grist_doc_context.tmp_loc
        if grist_doc_tmp_loc.type_location != TypeLocation.S3_FILE:
            raise ValueError(f"{GRIST_DOCUMENT_DATASET_NAME} destination must be an S3 location")

        local_document_path = GRIST_DOCUMENT_DIRECTORY / f"{document_id}.sqlite"
        if os.path.exists(local_document_path):
            logging.info(msg=f"Local Grist document already exists at {local_document_path}")
            return local_document_path

        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(),
        )
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=FSConfig(base_path=GRIST_DOCUMENT_DIRECTORY),
        )
        logging.info(
            msg=f"Downloading Grist from {grist_doc_tmp_loc.type_location}@{grist_doc_tmp_loc.validate_location} to {FileHandlerType.LOCAL}@{local_document_path}"
        )
        with s3_handler.read(file_path=grist_doc_tmp_loc.validate_location) as document_file:
            local_handler.write(file_path=local_document_path, content=document_file)

        return local_document_path

    def _read_data(
        self,
        nom_projet: str,
        datasets: tuple[Dataset, ...],
        execution_options: dict[str, ExecutionOptions],
        use_input_results_as_operation_args: bool,
    ) -> dict[str, pd.DataFrame]:
        logging.info(msg=f"{len(datasets)} datasets to read as input data")

        input_data = {}
        for index, dataset in enumerate(datasets):
            logging.info(msg=f"▶ {index + 1}/{len(datasets)} Reading dataset : {dataset.name}")
            exec_option = execution_options.get(dataset.name)
            if exec_option is None:
                raise ValueError(
                    f"No execution options found for dataset: {dataset.name}. Please check the configuration"
                )

            dataset_context = self.dataset_context_repo.get(nom_projet=nom_projet, nom_dataset=dataset.name)
            if use_input_results_as_operation_args:
                dataset_location = dataset_context.tmp_loc
            else:
                dataset_location = dataset_context.src_loc

            reader = self.location_provider_factory.create(dataset_location=dataset_location)
            logging.info(msg=f"Reading data from location: {dataset_location.validate_location}")
            read_options = exec_option.read_options
            if dataset_location.type_location == TypeLocation.GRIST:
                grist_document_path = self._download_grist_doc_locally(nom_projet=nom_projet)
                read_options = {
                    **read_options,
                    GRIST_SQLITE_PATH_READ_OPTION: grist_document_path,
                }

            df = reader.read(location=dataset_location.validate_location, read_options=read_options)
            logging.info(msg=f"Data read successfully. DataFrame shape: {df.shape}")
            input_data[f"df_{dataset.name}"] = df

        if len(input_data) == 1:
            input_data = {"df": next(iter(input_data.values()))}

        return input_data

    def _export_result(self, nom_projet: str, dataset: Dataset, result: object) -> None:
        output_dataset_context = self.dataset_context_repo.get(nom_projet=nom_projet, nom_dataset=dataset.name)
        output_location = output_dataset_context.dest_loc
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

    def run(
        self,
        nom_projet: str,
        pipeline: PipelineDescriptor,
        execution_options: dict[str, ExecutionOptions],
    ):
        # ===============================
        # Read data
        # ===============================
        logging.info(msg="Execution reading data step")
        if pipeline.input_datasets is None:
            input_data = {}
        else:
            input_data = self._read_data(
                nom_projet=nom_projet,
                datasets=pipeline.input_datasets,
                execution_options=execution_options,
                use_input_results_as_operation_args=pipeline.use_input_results_as_operation_args,
            )
        logging.info(msg="Reading data step completed successfully")

        # ===============================
        # Execution operation data
        # ===============================
        logging.info(msg="Running pipeline operation function")
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
        self._export_result(nom_projet=nom_projet, dataset=pipeline.output_dataset, result=result)
