from modules.domain.pipeline.model import ExecutionOptions, LoadStrategy

dag_id_fcu = "eligibilite_fcu"
nom_projet_fcu = "France Chaleur Urbaine (FCU)"

execution_options = {
    "fcu": ExecutionOptions(),
    "fcu_result": ExecutionOptions(load_strategy=LoadStrategy.APPEND),
}
