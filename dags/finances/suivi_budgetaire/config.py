from pathlib import Path

from modules.domain.pipeline.model import ExecutionOptions

dag_id_suivi_budgetaire = "suivi_budgetaire"
projet_suivi_budgetaire = "Suivi programmation budgétaire"
execution_options = {
    "nomenclature": ExecutionOptions(),
    "programmation_budgetaire": ExecutionOptions(),
    "consommation_budgetaire": ExecutionOptions(),
    "lfi": ExecutionOptions(),
}

config_projet_subpath = Path("dags/finances/suivi_budgetaire")
