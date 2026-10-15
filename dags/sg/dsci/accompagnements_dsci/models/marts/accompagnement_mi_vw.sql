
{{ config(materialized='view') }}

select
    adami.intitule,
    adami.date_de_realisation,
    adami.statut,
    adami.est_certifiant,
    adami.nb_inscrits,
    adami.est_ouvert_notation,
    adamisat.id_accompagnement,
    adamisat.nombre_de_participants,
    adamisat.nombre_de_reponses,
    adamisat.taux_de_reponse,
    adamisat.note_moyenne_de_satisfaction,
    acdrd.direction,
    adrta.type_d_accompagnement
from {{ source('activite_dsci', 'accompagnement_mi') }} as adami
left join {{ source('activite_dsci', 'accompagnement_mi_satisfaction') }} as adamisat
    on adami.id = adamisat.id_accompagnement
    and adami.snapshot_id = adamisat.snapshot_id
left join {{ source('activite_dsci', 'ref_direction') }} as acdrd
    on acdrd.id = adami.id_direction
    and acdrd.snapshot_id = adami.snapshot_id
left join {{ source('activite_dsci', 'ref_type_accompagnement') }} as adrta
    on adrta.id = adami.id_type_d_accompagnement
    and adrta.snapshot_id = adami.snapshot_id
