from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from modules.domain.selecteur.model import SelecteurConfig
from modules.domain.task.data_readers import DataContext, ReaderStrategy
from modules.infra.database.factory import DatabaseType, DbConfig, create_db_handler
from modules.infra.file_system.factory import FileHandlerType, FSConfig, create_file_handler


@dataclass(frozen=True)
class GristReaderStrategy(ReaderStrategy):
    doc_selecteur_name: str = "grist_doc"

    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext:

        if selecteurs is None:
            raise ValueError("selecteurs mapping is required for GristReaderStrategy")
        if self.doc_selecteur_name not in selecteurs:
            raise ValueError(f"Document selecteur '{self.doc_selecteur_name}' not found in runtime configs")

        if selecteur.id_source is None:
            raise ValueError(f"id_source must be defined for '{selecteur.selecteur}'.")

        # Handlers
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(
                bucket=selecteur.bucket,
                connection_id=selecteur.execution_options.s3_conn_id,
            ),
        )
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=FSConfig(
                base_path="/tmp",
            ),
        )

        doc_local_path = Path("/tmp") / selecteur.filename

        sqlite_handler = create_db_handler(
            db_type=DatabaseType.SQLITE,
            db_config=DbConfig(
                db_path=str(doc_local_path),
            ),
        )

        # Download the Grist document locally
        grist_doc = s3_handler.read(file_path=selecteur.get_full_s3_key(with_tmp_segment=True))

        local_handler.write(
            file_path=str(doc_local_path),
            content=grist_doc,
        )

        # Read the requested table
        df = sqlite_handler.fetch_df(query=f"SELECT * FROM {selecteur.id_source}")

        self.context.add(name=selecteur.selecteur, df=df)
        return self.context
