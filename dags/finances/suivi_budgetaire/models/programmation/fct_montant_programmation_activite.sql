
select
	-- Programmation annuelle
	prog_exec.exercice,
	prog_exec.numero_programme,
	prog_exec.bop,
	prog_exec.type,
	prog_exec.brique,
	prog_exec.action,
	prog_exec.numero_activite,
	prog_exec.type_montant,
	prog_exec.montant_execution,
	prog_exec.type_depense,
	prog_exec.rdv_budgetaire,
	prog_exec.import_timestamp,
	-- Dimension activite
	dim_activite.programme_brique,
	dim_activite.libelle_brique,
	dim_activite.libelle_activite
from {{ ref('fct_montant_programmation_unpivot') }} as prog_exec
left join {{ ref('dim_activite') }} as dim_activite
    on prog_exec.exercice = dim_activite.exercice
    and prog_exec.numero_activite = dim_activite.numero_activite
    and prog_exec.import_timestamp = dim_activite.import_timestamp
order by
    prog_exec.import_timestamp,
    prog_exec.exercice,
    prog_exec.bop,
    prog_exec."action",
    prog_exec.numero_activite
