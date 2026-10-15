
{{ config(materialized='view') }}

with cte_infos_correspondant as (
    select
        adcp.id_correspondant,
        adcp.snapshot_id,
        adrpc.profil_correspondant as type_de_correspondant,
        ref_c.competence as competence_particuliere,
        ccc.connaissance_communaute
    from {{ source('activite_dsci', 'correspondant_profil') }} as adcp
    left join {{ source('activite_dsci', 'ref_profil_correspondant') }} as adrpc
        on adrpc.id = adcp.id_type_de_correspondant
        and adrpc.snapshot_id = adcp.snapshot_id
    left join {{ source('activite_dsci', 'correspondant_competence_particuliere') }} as ccp
        on ccp.id_correspondant = adcp.id_correspondant
        and ccp.snapshot_id = adcp.snapshot_id
    left join {{ source('activite_dsci', 'ref_competence_particuliere') }} as ref_c
        on ref_c.id = ccp.id_competence_particuliere
        and ref_c.snapshot_id = adcp.snapshot_id
    left join {{ source('activite_dsci', 'correspondant_connaissance_communaute') }} as ccc
        on ccc.id_correspondant = adcp.id_correspondant
        and ccc.snapshot_id = adcp.snapshot_id
)

select
    c.*,
    ctei.type_de_correspondant,
    ctei.competence_particuliere,
    ctei.connaissance_communaute,
    rd.direction
from {{ source('activite_dsci', 'correspondant') }} as c
left join cte_infos_correspondant as ctei
    on ctei.id_correspondant = c.id
    and ctei.snapshot_id = c.snapshot_id
left join {{ source('activite_dsci', 'ref_direction') }} as rd
    on rd.id = c.id_direction
    and rd.snapshot_id = c.snapshot_id
