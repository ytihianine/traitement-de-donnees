from pathlib import Path

from modules.domain.pipeline.model import ExecutionOptions

execution_options = {
    "demande": ExecutionOptions(tbl_order=5),
    "equipe": ExecutionOptions(tbl_order=3),
    # Référentiels
    "ref_direction": ExecutionOptions(tbl_order=1),
    "ref_type_demande": ExecutionOptions(tbl_order=1),
}

# Variables
dag_id_demo = "demo_traitement_donnees"
projet_demo = "Démo traitement données"
config_projet_subpath = Path("dags/sg/dsci/demo")
