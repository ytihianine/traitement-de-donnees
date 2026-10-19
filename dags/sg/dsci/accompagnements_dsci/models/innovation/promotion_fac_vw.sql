{{ config(materialized='table') }}

select
    c.*,
    ami.intitule as promotion_fac
from {{ source('experimentation_ia', 'correspondant') }} as c
left join {{ source('experimentation_ia', 'accompagnement_mi') }} as ami
    on ami.id = c.id_promotion_fac
    and ami.snapshot_id = c.snapshot_id
