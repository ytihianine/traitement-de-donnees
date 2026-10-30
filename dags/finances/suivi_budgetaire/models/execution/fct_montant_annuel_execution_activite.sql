
select
    -- Execution mensuelle
    exec_mens.exercice,
    exec_mens.programme_code,
    exec_mens.activite_code,
    exec_mens.type_montant,
    SUM(exec_mens.montant_execution) as montant_execution,
    exec_mens.import_timestamp,
    -- Dimension activite
    dim_activite.programme_brique,
    dim_activite.brique,
    dim_activite.brique_libelle,
    dim_activite.activite_libelle
from {{ source('suivi_budgetaire', 'execution_mensuelle') }} as exec_mens
left join {{ ref('dim_activite') }} as dim_activite
    on exec_mens.exercice = dim_activite.exercice
    and exec_mens.activite_code = dim_activite.activite_code
    and exec_mens.import_timestamp = dim_activite.import_timestamp
group by
    exec_mens.exercice,
    exec_mens.programme_code,
    exec_mens.activite_code,
    exec_mens.type_montant,
    exec_mens.import_timestamp,
    dim_activite.programme_brique,
    dim_activite.brique,
    dim_activite.brique_libelle,
    dim_activite.activite_libelle
order by
    exec_mens.exercice,
    exec_mens.programme_code,
    exec_mens.activite_code,
    exec_mens.type_montant
