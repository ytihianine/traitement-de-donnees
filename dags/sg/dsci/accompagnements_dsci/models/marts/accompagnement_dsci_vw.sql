
{{ config(materialized='view') }}

select
    acc.*,
    ref.direction as nom_direction,
    effectif.nom_complet as porteur_dsci,
    ref_bureau.bureau as equipe_dsci,
    ref_typ.typologie_accompagnement
from {{ source('activite_dsci', 'accompagnement_dsci') }} as acc
left join {{ source('activite_dsci', 'ref_direction') }} as ref
    on acc.id_direction = ref.id
    and acc.snapshot_id = ref.snapshot_id
left join {{ source('activite_dsci', 'accompagnement_dsci_porteur') }} as bridge_porteur
    on acc.id = bridge_porteur.id_accompagnement
    and acc.snapshot_id = bridge_porteur.snapshot_id
left join {{ source('activite_dsci', 'effectif_dsci') }} as effectif
    on bridge_porteur.id_porteur_dsci = effectif.id
    and acc.snapshot_id = effectif.snapshot_id
left join {{ source('activite_dsci', 'accompagnement_dsci_equipe') }} as bridge_equipe
    on acc.id = bridge_equipe.id_accompagnement
    and acc.snapshot_id = bridge_equipe.snapshot_id
left join {{ source('activite_dsci', 'ref_bureau') }} as ref_bureau
    on bridge_equipe.id_equipe_s_dsci = ref_bureau.id
    and acc.snapshot_id = ref_bureau.snapshot_id
left join {{ source('activite_dsci', 'accompagnement_dsci_typologie') }} as bridge_typ
    on acc.id = bridge_typ.id_accompagnement
    and acc.snapshot_id = bridge_typ.snapshot_id
left join {{ source('activite_dsci', 'ref_typologie_accompagnement') }} as ref_typ
    on bridge_typ.id_typologie = ref_typ.id
    and acc.snapshot_id = ref_typ.snapshot_id
