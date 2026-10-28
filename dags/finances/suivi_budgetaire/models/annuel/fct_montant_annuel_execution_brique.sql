
select
    -- Execution mensuelle
    exec_mens.exercice,
    exec_mens.programme,
    exec_mens.type_montant,
    SUM(exec_mens.montant_execution) as montant_execution,
    exec_mens.import_timestamp,
    -- Dimension activite
    dim_activite.programme_brique,
    dim_activite.brique,
    dim_activite.libelle_brique
from {{ source('suivi_budgetaire', 'execution_mensuelle') }} as exec_mens
left join {{ ref('dim_activite') }} as dim_activite
    on exec_mens.programme = dim_activite.programme
    and exec_mens.brique = dim_activite.brique
    and exec_mens.numero_activite = dim_activite.numero_activite
group by
    exec_mens.exercice,
    exec_mens.programme,
    exec_mens.type_montant,
    exec_mens.import_timestamp,
    dim_activite.programme_brique,
    dim_activite.brique,
    dim_activite.libelle_brique
order by
    exec_mens.exercice,
    exec_mens.programme,
    exec_mens.type_montant
