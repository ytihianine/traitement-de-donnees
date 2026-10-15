
{{ config(materialized='view') }}

select
    afc.*,
    rf.competence as certification_souhaitee
from {{ source('activite_dsci', 'animateur_fac_certification') }} as afc
left join {{ source('activite_dsci', 'animateur_fac') }} as af
    on af.id = afc.id_animateur_fac
    and af.snapshot_id = afc.snapshot_id
left join {{ source('activite_dsci', 'ref_certification') }} as rf
    on rf.id = afc.id_certifications_souhaitees
    and rf.snapshot_id = afc.snapshot_id
