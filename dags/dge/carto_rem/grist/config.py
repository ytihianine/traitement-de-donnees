from modules.domain.pipeline.model import ExecutionOptions

execution_options = {
    "grist_doc": ExecutionOptions(),
    # Référentiels
    "ref_base_remuneration": ExecutionOptions(tbl_order=1),
    "ref_base_revalorisation": ExecutionOptions(tbl_order=1),
    "ref_niveau_diplome": ExecutionOptions(tbl_order=1),
    "ref_valeur_point_indice": ExecutionOptions(tbl_order=1),
    "ref_categorie_ecole": ExecutionOptions(tbl_order=1),
    "ref_libelle_diplome": ExecutionOptions(tbl_order=2),
    "ref_position": ExecutionOptions(tbl_order=2),
    "ref_fonction_dge": ExecutionOptions(tbl_order=1),
    # Sources Grist
    "agent": ExecutionOptions(tbl_order=3),
    "agent_diplome": ExecutionOptions(tbl_order=3),
    "agent_revalorisation": ExecutionOptions(tbl_order=3),
    "agent_revalorisation_proposition": ExecutionOptions(tbl_order=3),
    "agent_contrat_complement": ExecutionOptions(tbl_order=3),
    "agent_remuneration_complement": ExecutionOptions(tbl_order=3),
    "agent_experience_pro": ExecutionOptions(tbl_order=3),
    # Load to Grist (disabled)
    "get_agent_db": ExecutionOptions(),
    "load_agent": ExecutionOptions(),
}
