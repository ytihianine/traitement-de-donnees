
{{ config(materialized='view') }}

select
    acci.*,
    adsci.intitule_de_l_accompagnement
from {{ source('activite_dsci', 'accompagnement_cci_opportunite') }} as acci
left join {{ source('activite_dsci', 'accompagnement_dsci') }} as adsci
    on adsci.id = acci.id_accompagnement
    and adsci.snapshot_id = acci.snapshot_id
