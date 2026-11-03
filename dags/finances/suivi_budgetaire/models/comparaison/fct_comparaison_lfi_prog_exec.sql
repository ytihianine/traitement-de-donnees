
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
    prog_exec.montant_prog_execution,
    prog_exec.rdv_budgetaire
from {{ ref('fct_montant_annuel_lfi_brique') }} as lfi
left join {{ ref('fct_montant_programmation_brique') }} as prog_exec
    on prog_exec.import_timestamp = lfi.import_timestamp
    and prog_exec.exercice = lfi.exercice
    and prog_exec.programme_code = lfi.programme_code
    and prog_exec.programme_brique = lfi.programme_brique
    and prog_exec.type_montant = lfi.type_montant
order by
	prog_exec.import_timestamp,
	prog_exec.exercice,
	prog_exec.programme_brique,
	prog_exec.type_montant,
	prog_exec.montant_prog_execution
