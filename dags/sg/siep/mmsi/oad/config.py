from modules.domain.pipeline.model import ExecutionOptions, LoadStrategy

nom_projet_oad = "Outil aide diagnostic"
dag_id_oad = "outil_aide_diagnostic"

execution_options = {
    "accessibilite": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "accessibilite_detail": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "bacs": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "bails": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "biens": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "biens_gest": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "biens_occupants": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=3),
    "couts": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "deet_energie_ges": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "effectif": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "etat_de_sante": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "exploitation": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "gestionnaires": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "localisation": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "note": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "oad_carac": ExecutionOptions(),
    "oad_indic": ExecutionOptions(),
    "proprietaire": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "reglementation": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "sites": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=1),
    "strategie": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "surface": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "typologie": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
    "valeur": ExecutionOptions(load_strategy=LoadStrategy.APPEND, tbl_order=2),
}
