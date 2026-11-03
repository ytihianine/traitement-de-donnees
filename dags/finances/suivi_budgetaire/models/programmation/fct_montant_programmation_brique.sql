
select
	-- Programmation annuelle
	prog_exec.exercice,
	prog_exec.programme_code,
	prog_exec.brique_code,
	prog_exec.type_montant,
	SUM(prog_exec.montant_prog_execution) as montant_prog_execution,
	prog_exec.rdv_budgetaire,
	prog_exec.import_timestamp,
	-- Dimension activite
	prog_exec.programme_brique,
	prog_exec.brique_libelle
from {{ ref('fct_montant_programmation_activite') }} as prog_exec
group by
	prog_exec.exercice,
	prog_exec.programme_code,
	prog_exec.brique_code,
	prog_exec.type_montant,
	prog_exec.rdv_budgetaire,
	prog_exec.import_timestamp,
	prog_exec.programme_brique,
	prog_exec.brique_libelle
order by
    prog_exec.import_timestamp,
    prog_exec.exercice,
    prog_exec.programme_code,
	prog_exec.rdv_budgetaire,
	prog_exec.type_montant,
	montant_prog_execution
