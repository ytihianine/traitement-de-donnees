
{{ config(materialized='view') }}

select
    efdsci.*,
    rfb.bureau,
    rfp.pole
from {{ source('activite_dsci', 'effectif_dsci') }} as efdsci
left join {{ source('activite_dsci', 'ref_bureau') }} as rfb
    on efdsci.id_bureau = rfb.id
    and efdsci.snapshot_id = rfb.snapshot_id
left join {{ source('activite_dsci', 'ref_pole') }} as rfp
    on efdsci.id_pole = rfp.id
    and efdsci.snapshot_id = rfp.snapshot_id
