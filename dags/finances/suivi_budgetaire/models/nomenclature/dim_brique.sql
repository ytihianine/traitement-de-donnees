

select distinct
    nomenclature.exercice,
    nomenclature.programme_code,
    concat(nomenclature.programme_code, '-', nomenclature.brique_code) as programme_brique,
    nomenclature.brique_code,
    nomenclature.brique_libelle,
    nomenclature.import_timestamp,
    nomenclature.snapshot_id,
    nomenclature.snapshot_id_parent
from {{ source('suivi_budgetaire', 'nomenclature') }} as nomenclature
where nomenclature.brique_code is not null
