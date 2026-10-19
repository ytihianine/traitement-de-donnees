
{{ config(materialized='view') }}

select
    ai.*,
    acc.intitule,
    eff.nom_complet
from {{ source('activite_dsci', 'animateur_interne') }} as ai
left join {{ source('activite_dsci', 'accompagnement_mi') }} as acc
    on acc.id = ai.id_accompagnement
    and acc.snapshot_id = ai.snapshot_id
left join {{ source('activite_dsci', 'effectif_dsci') }} as eff
    on eff.id = ai.id_animateur
    and eff.snapshot_id = ai.snapshot_id
