
{{ config(materialized='view') }}

select
    ref.direction as nom_direction,
    acc.statut,
    count(*) as nb_projets
from {{ source('activite_dsci', 'accompagnement_dsci') }} as acc
left join {{ source('activite_dsci', 'ref_direction') }} as ref
    on acc.id_direction = ref.id
    and acc.snapshot_id = ref.snapshot_id
group by ref.direction, acc.statut
order by nb_projets desc
