
{{ config(materialized='view') }}

select
    accp.*,
    effectif.nom_complet as porteur_dsci
from {{ source('activite_dsci', 'accompagnement_dsci_porteur') }} as accp
left join {{ source('activite_dsci', 'effectif_dsci') }} as effectif
    on accp.id_accompagnement = effectif.id
