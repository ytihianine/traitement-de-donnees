
select
    -- Programmation execution
    prog_exec.exercice,
    prog_exec.programme_code,
    prog_exec.type_montant,
    prog_exec.montant_prog_execution,
    prog_exec.import_timestamp,
    prog_exec.programme_brique,
    prog_exec.brique,
    prog_exec.libelle_brique,
    -- LFI
    lfi.montant_lfi
from {{ ref('fct_montant_programmation_brique') }} as prog_exec
left join {{ ref('fct_montant_annuel_lfi_brique') }} as lfi
    on prog_exec.exercice = lfi.exercice
    and prog_exec.brique = lfi.brique
    and prog_exec.type_montant = lfi.type_montant
    and prog_exec.import_timestamp = lfi.import_timestamp
order by
	prog_exec.import_timestamp,
	prog_exec.exercice,
	prog_exec.programme_code,
	prog_exec.brique,
	prog_exec.type_montant,
	prog_exec.montant_prog_execution
