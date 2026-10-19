
{{ config(materialized='view') }}

select
    afac.*,
    acc.intitule as accompagnement,
    rco.nom_complet as nom_animateur,
    rfc.competence as certification_souhaitee
from {{ source('activite_dsci', 'animateur_fac') }} as afac
left join {{ source('activite_dsci', 'accompagnement_mi') }} as acc
    on acc.id = afac.id_accompagnement
    and acc.snapshot_id = afac.snapshot_id
left join {{ source('activite_dsci', 'correspondant') }} as rco
    on rco.id = afac.id_animateur
    and rco.snapshot_id = afac.snapshot_id
left join {{ source('activite_dsci', 'animateur_fac_certification') }} as afaccer
    on afaccer.id_animateur_fac = afac.id
    and afaccer.snapshot_id = afac.snapshot_id
left join {{ source('activite_dsci', 'ref_certification') }} as rfc
    on rfc.id = afaccer.id_certifications_souhaitees
    and rfc.snapshot_id = afac.snapshot_id
