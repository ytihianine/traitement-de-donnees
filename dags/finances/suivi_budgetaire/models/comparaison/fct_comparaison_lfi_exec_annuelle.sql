
select
    -- LFI
	lfi.exercice,
	lfi.programme_code,
	lfi.programme_libelle,
	lfi.brique_code,
	lfi.brique_libelle,
	lfi.type_montant,
	lfi.montant_lfi,
	lfi.import_timestamp,
    lfi.programme_brique,
    -- Execution annuelle
    exec_annuelle.montant_execution
from {{ ref('fct_montant_annuel_lfi_brique') }} as lfi
left join {{ ref('fct_montant_annuel_execution_brique') }} as exec_annuelle
    on exec_annuelle.import_timestamp = lfi.import_timestamp
    and exec_annuelle.exercice = lfi.exercice
    and exec_annuelle.programme_code = lfi.programme_code
    and exec_annuelle.programme_brique = lfi.programme_brique
    and exec_annuelle.type_montant = lfi.type_montant
order by
	exec_annuelle.import_timestamp,
	exec_annuelle.exercice,
	exec_annuelle.programme_code,
	exec_annuelle.type_montant,
	exec_annuelle.montant_execution
