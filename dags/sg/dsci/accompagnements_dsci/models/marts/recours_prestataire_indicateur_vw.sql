
{{ config(materialized='view') }}

select
    ref.direction as nom_direction,
    case when acc.recours_prestataire = 'Oui' then 100.0 else 0.0 end as indicateur_presta
from {{ source('activite_dsci', 'accompagnement_dsci') }} as acc
left join {{ source('activite_dsci', 'ref_direction') }} as ref
    on acc.id_direction = ref.id
    and acc.snapshot_id = ref.snapshot_id
