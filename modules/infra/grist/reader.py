from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from modules.domain.dataset.model import Dataset
from modules.domain.dataset.ports import DatasetReader
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@dataclass(frozen=True)
class GristReaderStrategy(DatasetReader):
    fs_config: FSConfig
    fs_type: FileHandlerType = FileHandlerType.S3
    doc_selecteur_name: str = "grist_doc"

    def read(
        self,
        dataset: Dataset,
    ) -> pd.DataFrame:

        if dataset.storage.id_source is None:
            raise ValueError(f"id_source must be defined for '{dataset.name}'.")

        # Handlers
        s3_handler = create_file_handler(
            handler_type=self.fs_type,
            config=self.fs_config,
        )
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=FSConfig(
                base_path="/tmp",
            ),
        )

        doc_local_path = Path("/tmp") / dataset.storage.filename

        sqlite_handler = create_db_handler(
            db_type=DatabaseType.SQLITE,
            db_config=DbConfig(
                db_path=str(doc_local_path),
            ),
        )

        # Download the Grist document locally
        grist_doc = s3_handler.read(file_path=dataset.storage.get_full_s3_key(with_tmp_segment=True))

        local_handler.write(
            file_path=str(doc_local_path),
            content=grist_doc,
        )

        # Read the requested table
        df = sqlite_handler.fetch_df(query=f"SELECT * FROM {dataset.storage.id_source}")

        return df
