

select distinct
    nomenclature.exercice,
    nomenclature.programme,
    nomenclature.brique,
    nomenclature.activite as numero_activite,
    concat(nomenclature.programme, '-', nomenclature.brique) as programme_brique,
    nomenclature.libelle_brique as libelle_brique,
    nomenclature.activite,
    nomenclature.libelle_activite as libelle_activite,
    nomenclature.domaine_fonctionnel,
    nomenclature.libelle_df as libelle_df
from {{ source('suivi_budgetaire', 'nomenclature') }} as nomenclature
