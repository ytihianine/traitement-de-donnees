
select
    -- Execution mensuelle
    exec_mens.exercice,
    exec_mens.date_execution,
    concat(to_char(exec_mens.date_execution, 'MM'), '-', exec_mens.mois) as mois_label,
    exec_mens.programme_code,
    exec_mens.brique_code,
    exec_mens.type_montant,
    sum(montant_execution) as montant_execution,
    exec_mens.import_timestamp,
    -- Dimension activite
    dim_activite.programme_brique,
    dim_activite.brique_libelle
from {{ source('suivi_budgetaire', 'execution_mensuelle') }} exec_mens
left join {{ ref('dim_activite') }} as dim_activite
    on exec_mens.import_timestamp = dim_activite.import_timestamp
    and exec_mens.exercice = dim_activite.exercice
    and exec_mens.brique_code = dim_activite.brique_code
group by
    exec_mens.import_timestamp,
    exec_mens.exercice,
    exec_mens.date_execution,
    exec_mens.mois,
    exec_mens.programme_code,
    exec_mens.brique_code,
    exec_mens.type_montant,
    dim_activite.programme_brique,
    dim_activite.brique_libelle
order by
    exec_mens.import_timestamp,
    exec_mens.exercice,
    exec_mens.date_execution,
    exec_mens.programme_code,
    exec_mens.type_montant,
    sum(montant_execution)
