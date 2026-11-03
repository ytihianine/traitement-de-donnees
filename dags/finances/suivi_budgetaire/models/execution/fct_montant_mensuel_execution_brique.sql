
select
    -- Execution mensuelle
    exec_mens.exercice,
    exec_mens.date_execution,
    exec_mens.mois_label,
    exec_mens.programme_code,
    exec_mens.type_montant,
    sum(exec_mens.montant_execution) as montant_execution,
    exec_mens.import_timestamp,
    exec_mens.programme_brique,
    exec_mens.brique_libelle
from {{ ref('fct_montant_mensuel_execution_activite') }} as exec_mens
group by
    exec_mens.import_timestamp,
    exec_mens.exercice,
    exec_mens.date_execution,
    exec_mens.mois_label,
    exec_mens.programme_code,
    exec_mens.type_montant,
    exec_mens.programme_brique,
    exec_mens.brique_libelle
order by
    exec_mens.import_timestamp,
    exec_mens.exercice,
    exec_mens.date_execution,
    exec_mens.programme_code,
    exec_mens.type_montant,
    montant_execution
