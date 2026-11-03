
select
    -- Execution mensuelle
    exec_mens_br.exercice,
    exec_mens_br.programme_code,
    exec_mens_br.type_montant,
    SUM(exec_mens_br.montant_execution) as montant_execution,
    exec_mens_br.import_timestamp,
    -- Dimension activite
    exec_mens_br.programme_brique,
    exec_mens_br.brique_libelle
from {{ ref('fct_montant_mensuel_execution_brique') }} as exec_mens_br
group by
    exec_mens_br.import_timestamp,
    exec_mens_br.exercice,
    exec_mens_br.programme_code,
    exec_mens_br.programme_brique,
    exec_mens_br.brique_libelle,
    exec_mens_br.type_montant
order by
    exec_mens_br.exercice,
    exec_mens_br.programme_code,
    exec_mens_br.type_montant
