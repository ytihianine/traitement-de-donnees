from modules.domain.pipeline.model import ExecutionOptions

execution_options = {
    "accompagnement": ExecutionOptions(tbl_order=5),
    # Référentiels
    "ref_direction": ExecutionOptions(tbl_order=1),
    "ref_intervention": ExecutionOptions(tbl_order=1),
}
