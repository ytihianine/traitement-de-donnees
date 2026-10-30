

select distinct
    nomenclature.exercice,
    nomenclature.programme_code,
    concat(nomenclature.programme_code, '-', nomenclature.brique_code) as programme_brique,
    nomenclature.brique_code,
    nomenclature.brique_libelle,
    nomenclature.activite_code,
    nomenclature.activite_libelle,
    nomenclature.domaine_fonctionnel,
    nomenclature.libelle_df,
    nomenclature.uo,
    nomenclature.bop,
    nomenclature.import_timestamp,
    nomenclature.snapshot_id,
    nomenclature.snapshot_id_parent
from {{ source('suivi_budgetaire', 'nomenclature') }} as nomenclature
