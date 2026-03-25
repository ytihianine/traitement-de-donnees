from _types.projet import SelecteurStorageOptions

# Default NULL values
DEFAULT_NULL_CC_CF = "Ind"

selecteur_options = {
    "delai_global_paiement": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "demande_achat": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "demande_paiement": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "demande_paiement_carte_achat": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "demande_paiement_complet": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "demande_paiement_flux": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "demande_paiement_journal_pieces": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "demande_paiement_sfp": SelecteurStorageOptions(
        is_partitioned=False,
    ),
    "engagement_juridique": SelecteurStorageOptions(
        is_partitioned=False,
    ),
}
