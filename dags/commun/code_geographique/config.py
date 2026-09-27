from modules.domain.pipeline.model import ExecutionOptions, LoadStrategy

execution_options = {
    "code_iso_departement": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "code_iso_region": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "communes": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "departements": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "departements_geojson": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "regions": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
    "regions_geojson": ExecutionOptions(load_strategy=LoadStrategy.FULL_LOAD),
}
