with rdv_budgetaire_distinct as (
    select distinct
        prog.rdv_budgetaire
    from {{ ref('fct_montant_programmation_brique') }} as prog
),
lfi_rdv as (
    select
        lfi.exercice,
        lfi.programme_code,
        lfi.programme_libelle,
        lfi.brique_code,
        lfi.brique_libelle,
        lfi.programme_brique,
        lfi.type_montant,
        lfi.import_timestamp,
        rdv.rdv_budgetaire,
        lfi.montant_lfi as montant,
        'LFI'::text as source_montant
    from {{ ref('fct_montant_annuel_lfi_brique') }} as lfi
    cross join rdv_budgetaire_distinct as rdv
),
prog_exec_rdv as (
    select
        prog.exercice,
        prog.programme_code,
        nullif(prog.programme_brique, '') as programme_brique,
        prog.brique_code,
        prog.brique_libelle,
        prog.type_montant,
        prog.import_timestamp,
        prog.rdv_budgetaire,
        sum(prog.montant_prog_execution) as montant,
        'prog_execution'::text as source_montant
    from {{ ref('fct_montant_programmation_brique') }} as prog
    group by
        prog.exercice,
        prog.programme_code,
        prog.programme_brique,
        prog.brique_code,
        prog.brique_libelle,
        prog.type_montant,
        prog.import_timestamp,
        prog.rdv_budgetaire
)
select
    exercice,
    programme_code,
    programme_libelle,
    brique_code,
    brique_libelle,
    programme_brique,
    type_montant,
    import_timestamp,
    rdv_budgetaire,
    montant,
    source_montant
from lfi_rdv

union all

select
    exercice,
    programme_code,
    null as programme_libelle,
    brique_code,
    brique_libelle,
    programme_brique,
    type_montant,
    import_timestamp,
    rdv_budgetaire,
    montant,
    source_montant
from prog_exec_rdv
order by
    exercice,
    rdv_budgetaire,
    source_montant,
    programme_code,
    brique_code
