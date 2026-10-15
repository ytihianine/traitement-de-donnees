from pathlib import Path

from modules.domain.pipeline.model import ExecutionOptions, LoadStrategy

nom_projet_georisque = "Géorisques"
dag_id_georisque = "georisques_batiments"
config_projet_subpath = Path("dags/sg/siep/mmsi/georisques")

execution_options = {
    "bien_db": ExecutionOptions(),
    "georisques": ExecutionOptions(load_strategy=LoadStrategy.APPEND),
}
