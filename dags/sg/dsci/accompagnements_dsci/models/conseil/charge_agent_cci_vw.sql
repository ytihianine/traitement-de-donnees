
{{ config(materialized='view') }}

select
    ag.*,
    rfa.nom_complet,
    rfs.semaine,
    adsci.intitule_de_l_accompagnement
from {{ source('activite_dsci', 'charge_agent_cci') }} as ag
left join {{ source('activite_dsci', 'effectif_dsci') }} as rfa
    on rfa.id = ag.id_agent_e_
    and rfa.snapshot_id = ag.snapshot_id
left join {{ source('activite_dsci', 'ref_semainier') }} as rfs
    on rfs.id = ag.id_semaine
    and rfs.snapshot_id = ag.snapshot_id
left join {{ source('activite_dsci', 'accompagnement_dsci') }} as adsci
    on adsci.id = ag.id_missions
    and adsci.snapshot_id = ag.snapshot_id
