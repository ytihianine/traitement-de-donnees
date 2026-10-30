
select
	-- Programmation annuelle
	prog_exec.exercice,
	prog_exec.programme_code,
	prog_exec.bop,
	prog_exec.type,
	prog_exec.brique,
	prog_exec.type_montant,
	prog_exec.montant_prog_execution,
	prog_exec.type_depense,
	prog_exec.rdv_budgetaire,
	prog_exec.import_timestamp,
	-- Dimension activite
	dim_brique.programme_brique,
	dim_brique.libelle_brique
from {{ ref('fct_montant_programmation_unpivot') }} as prog_exec
left join {{ ref('dim_brique') }} as dim_brique
    on prog_exec.exercice = dim_brique.exercice
    and prog_exec.brique = dim_brique.brique
    and prog_exec.import_timestamp = dim_brique.import_timestamp
group by
	prog_exec.exercice,
	prog_exec.programme_code,
	prog_exec.bop,
	prog_exec.type,
	prog_exec.brique,
	prog_exec.type_montant,
	prog_exec.montant_prog_execution,
	prog_exec.type_depense,
	prog_exec.rdv_budgetaire,
	prog_exec.import_timestamp,
	dim_brique.programme_brique,
	dim_brique.libelle_brique
order by
    prog_exec.import_timestamp,
    prog_exec.exercice,
    prog_exec.programme_code,
	prog_exec.brique,
	prog_exec.rdv_budgetaire,
	prog_exec.type_montant,
	prog_exec.montant_prog_execution
