from modules.domain.pipeline.model import ExecutionOptions, LoadStrategy

execution_options = {
    "agent": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD, read_options={"sep": ";"}),
    "certificat": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "mandataire": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
