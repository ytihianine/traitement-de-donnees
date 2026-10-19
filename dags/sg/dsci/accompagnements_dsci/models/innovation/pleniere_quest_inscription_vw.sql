
{{ config(materialized='view') }}

select
    piq.*,
    rfdir.direction,
    ami.intitule
from {{ source('activite_dsci', 'pleniere_quest_inscription') }} as piq
left join {{ source('activite_dsci', 'ref_direction') }} as rfdir
    on rfdir.id = piq.id_direction
    and rfdir.snapshot_id = piq.snapshot_id
left join {{ source('activite_dsci', 'accompagnement_mi') }} as ami
    on ami.id = piq.id_id_accompagnement
    and ami.snapshot_id = piq.snapshot_id
