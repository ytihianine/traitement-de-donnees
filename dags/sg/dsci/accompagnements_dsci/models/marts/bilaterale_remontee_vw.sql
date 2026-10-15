{{ config(materialized='table') }}

select
    b.date_de_rencontre,
    b.intitule as intitule_bilaterale,
    ref.direction,
    bureau,
    r.*
from {{ source('experimentation_ia', 'bilaterale') }} as b
left join {{ source('experimentation_ia', 'ref_direction') }} as ref
    on b.id_direction = ref.id
    and b.snapshot_id = ref.snapshot_id
left join {{ source('experimentation_ia', 'bilaterale_remontee') }} as r
    on b.id = r.id_bilaterale
    and b.snapshot_id = r.snapshot_id
left join {{ source('experimentation_ia', 'ref_bureau') }} as bureau
    on r.id_bureau = bureau.id
    and r.snapshot_id = bureau.snapshot_id
