
{{ config(materialized='view') }}

select
    c.*,
    ami.intitule as promotion_fac
from {{ source('activite_dsci', 'correspondant') }} as c
left join {{ source('activite_dsci', 'accompagnement_mi') }} as ami
    on ami.id = c.id_promotion_fac
    and ami.snapshot_id = c.snapshot_id
