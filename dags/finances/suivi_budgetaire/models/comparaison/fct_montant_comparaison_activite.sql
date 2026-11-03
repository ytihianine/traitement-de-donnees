
select
	-- Programmation annuelle
	prog_exec.exercice,
	prog_exec.programme_code,
	prog_exec.bop,
	prog_exec.type,
	prog_exec.brique_code,
	prog_exec.action,
	prog_exec.activite_code,
	prog_exec.type_montant,
	prog_exec.montant_prog_execution,
	prog_exec.type_depense,
	prog_exec.rdv_budgetaire,
	prog_exec.import_timestamp,
    -- Execution mensuelle
    exec_annuelle.montant_execution,
	-- Dimension activite
	dim_activite.programme_brique,
	dim_activite.brique_libelle,
	dim_activite.activite_libelle

from {{ ref('fct_montant_programmation_unpivot') }} as prog_exec
left join {{ ref('fct_montant_annuel_execution_activite') }} as exec_annuelle
    on prog_exec.exercice = exec_annuelle.exercice
    and prog_exec.activite_code = exec_annuelle.activite_code
    and prog_exec.type_montant = exec_annuelle.type_montant
    and prog_exec.import_timestamp = exec_annuelle.import_timestamp
left join {{ ref('dim_activite') }} as dim_activite
    on prog_exec.exercice = dim_activite.exercice
    and prog_exec.activite_code = dim_activite.activite_code
    and prog_exec.import_timestamp = dim_activite.import_timestamp
