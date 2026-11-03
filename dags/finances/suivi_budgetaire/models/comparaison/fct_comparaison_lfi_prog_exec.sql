
with rdv_budgetaire_par_brique as (
    select distinct
        prog.rdv_budgetaire
    from {{ ref('fct_montant_programmation_brique') }} as prog
),
prog_exec_agg as (
    select
        prog.exercice,
        prog.programme_code,
        prog.brique_code,
        prog.type_montant,
        prog.rdv_budgetaire,
        prog.import_timestamp,
        sum(prog.montant_prog_execution) as montant_prog_execution
    from {{ ref('fct_montant_programmation_brique') }} as prog
    group by
        prog.exercice,
        prog.programme_code,
        prog.brique_code,
        prog.type_montant,
        prog.rdv_budgetaire,
        prog.import_timestamp
),
lfi_expanded as (
    select
        lfi.exercice,
        lfi.programme_code,
        lfi.programme_libelle,
        lfi.brique_code,
        lfi.brique_libelle,
        lfi.type_montant,
        lfi.montant_lfi,
        lfi.import_timestamp,
        lfi.programme_brique,
        rdv.rdv_budgetaire
    from {{ ref('fct_montant_annuel_lfi_brique') }} as lfi
    cross join rdv_budgetaire_par_brique as rdv
)
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
	lfi.rdv_budgetaire,
    -- Execution annuelle
    prog_exec.montant_prog_execution
from lfi_expanded as lfi
left join prog_exec_agg as prog_exec
    on prog_exec.import_timestamp = lfi.import_timestamp
    and prog_exec.exercice = lfi.exercice
    and prog_exec.rdv_budgetaire = lfi.rdv_budgetaire
    and prog_exec.programme_code = lfi.programme_code
    and prog_exec.brique_code = lfi.brique_code
    and prog_exec.type_montant = lfi.type_montant
order by
	lfi.import_timestamp,
	lfi.exercice,
	lfi.programme_brique,
	lfi.rdv_budgetaire,
	lfi.type_montant,
	prog_exec.montant_prog_execution
