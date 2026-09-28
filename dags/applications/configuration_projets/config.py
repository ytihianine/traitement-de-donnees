from modules.domain.pipeline.model import ExecutionOptions

execution_options = {
    "ref_service": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "ref_direction": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "grist_doc": ExecutionOptions(),
    "projet_contact": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "projet_documentation": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "projet_s3": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "projet_selecteur": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "projets": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "selecteur_column_mapping": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "selecteur_database": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "selecteur_s3": ExecutionOptions(
        keep_file_id_col=False,
    ),
    "selecteur_source": ExecutionOptions(
        keep_file_id_col=False,
    ),
}
