

select distinct
    nomenclature.exercice,
    nomenclature.programme,
    concat(nomenclature.programme, '-', nomenclature.brique) as programme_brique,
    nomenclature.brique,
    nomenclature.libelle_brique as libelle_brique,
    nomenclature.activite,
    nomenclature.libelle_activite as libelle_activite,
    nomenclature.domaine_fonctionnel,
    nomenclature.libelle_df as libelle_df,
    nomenclature.uo,
    nomenclature.bop,
    nomenclature.import_timestamp,
    nomenclature.snapshot_id,
    nomenclature.snapshot_id_parent
from {{ source('suivi_budgetaire', 'nomenclature') }} as nomenclature
