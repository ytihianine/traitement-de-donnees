

select distinct
    nomenclature.exercice,
    nomenclature.programme,
    nomenclature.brique,
    nomenclature.activite as numero_activite,
    concat(nomenclature.programme, '-', nomenclature.brique) as programme_brique,
    nomenclature.libelle_brique as libelle_brique,
    nomenclature.domaine_fonctionnel,
    nomenclature.libelle_df as libelle_df,
    nomenclature.import_timestamp,
    nomenclature.snapshot_id,
    nomenclature.snapshot_id_parent
from {{ source('suivi_budgetaire', 'nomenclature') }} as nomenclature
