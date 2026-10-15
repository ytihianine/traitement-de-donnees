
{{ config(materialized='view') }}

select
    ae.*,
    acc.intitule
from {{ source('activite_dsci', 'animateur_externe') }} as ae
left join {{ source('activite_dsci', 'accompagnement_mi') }} as acc
    on acc.id = ae.id_accompagnement
    and acc.snapshot_id = ae.snapshot_id
