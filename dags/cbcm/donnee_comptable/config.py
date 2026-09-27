from modules.domain.pipeline.model import ExecutionOptions

# Default NULL values
DEFAULT_NULL_CC_CF = "Ind"

execution_options = {
    "delai_global_paiement": ExecutionOptions(
        is_partitioned=False,
        read_options={"skiprows": 3},
    ),
    "demande_achat": ExecutionOptions(
        is_partitioned=False,
        read_options={"skiprows": 3},
    ),
    "demande_paiement": ExecutionOptions(
        is_partitioned=False,
    ),
    "demande_paiement_carte_achat": ExecutionOptions(
        is_partitioned=False,
    ),
    "demande_paiement_complet": ExecutionOptions(
        is_partitioned=False,
    ),
    "demande_paiement_flux": ExecutionOptions(
        is_partitioned=False,
        read_options={"skiprows": 3},
    ),
    "demande_paiement_journal_pieces": ExecutionOptions(
        is_partitioned=False,
    ),
    "demande_paiement_sfp": ExecutionOptions(
        is_partitioned=False,
    ),
    "engagement_juridique": ExecutionOptions(
        is_partitioned=False,
    ),
}
