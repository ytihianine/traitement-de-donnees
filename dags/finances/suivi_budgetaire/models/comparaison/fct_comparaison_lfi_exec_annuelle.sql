
select
    -- Execution annuelle
    exec_annuelle.exercice,
    exec_annuelle.programme,
    exec_annuelle.type_montant,
    exec_annuelle.montant_execution,
    exec_annuelle.import_timestamp,
    exec_annuelle.programme_brique,
    exec_annuelle.brique,
    exec_annuelle.libelle_brique,
    -- LFI
    lfi.montant_lfi
from {{ ref('fct_montant_annuel_execution_brique') }} as exec_annuelle
left join {{ ref('fct_montant_annuel_lfi_brique') }} as lfi
    on exec_annuelle.exercice = lfi.exercice
    and exec_annuelle.brique = lfi.brique
    and exec_annuelle.type_montant = lfi.type_montant
    and exec_annuelle.import_timestamp = lfi.import_timestamp
order by
	exec_annuelle.import_timestamp,
	exec_annuelle.exercice,
	exec_annuelle.programme,
	exec_annuelle.brique,
	exec_annuelle.type_montant,
	exec_annuelle.montant_execution
