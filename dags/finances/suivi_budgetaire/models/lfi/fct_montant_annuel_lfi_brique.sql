
select
    -- lfi brique
	lfi.exercice,
	lfi.programme,
	lfi.libelle_programme,
	lfi.brique,
	lfi.libelle_brique,
	lfi.type_montant,
	lfi.montant_lfi,
	lfi.import_timestamp,
    -- Dimension brique
    dim_brique.programme_brique

from {{ source('suivi_budgetaire', 'lfi_brique') }} as lfi
left join {{ ref('dim_brique') }} as dim_brique
    on lfi.exercice = dim_brique.exercice
    and lfi.brique = dim_brique.brique
    and lfi.import_timestamp = dim_brique.import_timestamp
