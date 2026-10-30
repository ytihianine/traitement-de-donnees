
select
    -- Execution mensuelle
    exec_mens.exercice,
    exec_mens.date_execution,
    concat(to_char(exec_mens.date_execution, 'MM'), '-', exec_mens.mois) as mois_label,
    exec_mens.programme_code,
    exec_mens.activite_code,
    exec_mens.titre,
    exec_mens.fonds,
    exec_mens.type_montant,
    exec_mens.montant_execution,
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
order by
    exec_mens.exercice,
    exec_mens.date_execution,
    exec_mens.programme_code,
    exec_mens.activite_code,
    exec_mens.type_montant,
    exec_mens.montant_execution
