
{{ config(materialized='view') }}

select
    c.*,
    rr.region as nom_region
from {{ source('activite_dsci', 'correspondant') }} as c
left join {{ source('activite_dsci', 'ref_region') }} as rr
    on c.id_region = rr.id
    and c.snapshot_id = rr.snapshot_id
