from modules.enums.database import LoadStrategy
from modules.types.projet import SelecteurStorageOptions

# Default NULL values
DEFAULT_NULL_CC_CF = "Ind"

storage_options = {
    "delai_global_paiement": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
        read_options={"skiprows": 3},
    ),
    "demande_achat": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
        read_options={"skiprows": 3},
    ),
    "demande_paiement": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "demande_paiement_carte_achat": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "demande_paiement_complet": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "demande_paiement_flux": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
        read_options={"skiprows": 3},
    ),
    "demande_paiement_journal_pieces": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "demande_paiement_sfp": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
    ),
    "engagement_juridique": SelecteurStorageOptions(
        is_partitioned=False,
        load_strategy=LoadStrategy.APPEND,
    ),
}
