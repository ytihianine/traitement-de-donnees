from modules.domain.pipeline.model import ExecutionOptions, LoadStrategy

execution_options = {
    "budget_general": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "budget_ministere": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "budget_pilotable": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "budget_total": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "db_plafond_etpt": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "effectif_departements": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "effectif_direction": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "effectif_perimetre": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "election_resultat": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "engagement_agent": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "evolution_budget_mef": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "grist_doc": ExecutionOptions(),
    "masse_salariale": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "montant_intervention_invest": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "plafond_etpt": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "taux_participation": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "teletravail": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "teletravail_frequence": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "teletravail_opinion": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
