select
    prog_ann_ae.exercice,
    prog_ann_ae.programme,
    prog_ann_ae.bop,
    prog_ann_ae.type,
    prog_ann_ae.brique,
    prog_ann_ae.action,
    prog_ann_ae.code as numero_activite,
    'ae' as type_montant,
    prog_ann_ae.execution_ae as montant_execution,
    prog_ann_ae.type_depense,
    prog_ann_ae.rdv_budgetaire,
    prog_ann_ae.import_timestamp
from {{ source('suivi_budgetaire', 'programmation_annuelle') }} as prog_ann_ae
where
	prog_ann_ae.type in ('ATV')
union all
select
    prog_ann_cp.exercice,
    prog_ann_cp.programme,
    prog_ann_cp.bop,
    prog_ann_cp.type,
    prog_ann_cp.brique,
    prog_ann_cp.action,
    prog_ann_cp.code as numero_activite,
    'cp' as type_montant,
    prog_ann_cp.execution_cp as montant_execution,
    prog_ann_cp.type_depense,
    prog_ann_cp.rdv_budgetaire,
    prog_ann_cp.import_timestamp
from {{ source('suivi_budgetaire', 'programmation_annuelle') }} as prog_ann_cp
where
	prog_ann_cp.type in ('ATV')
