
{{ config(materialized='view') }}

with cte_region as (
    select
        adrr.id,
        adrr.region,
        crg.coordonnees,
        adrr.snapshot_id
    from {{ source('activite_dsci', 'ref_region') }} as adrr
    left join {{ source('commun', 'region_geojson') }} as crg
        on adrr.region = crg.libelle
),
cte_correspond_profil as (
    select
        adcp.id_correspondant,
        adpc.profil_correspondant,
        adcp.snapshot_id
    from {{ source('activite_dsci', 'correspondant_profil') }} as adcp
    left join {{ source('activite_dsci', 'ref_profil_correspondant') }} as adpc
        on adpc.id = adcp.id_type_de_correspondant
        and adpc.snapshot_id = adcp.snapshot_id
)

select
    adc.mail,
    adc.actif,
    adc.date_debut_inactivite,
    adc.est_certifie_fac,
    adc.actif_communaute_fac,
    ctecp.profil_correspondant,
    adrd.direction,
    ctereg.region,
    ctereg.coordonnees
from {{ source('activite_dsci', 'correspondant') }} as adc
left join cte_correspond_profil as ctecp
    on ctecp.id_correspondant = adc.id
    and ctecp.snapshot_id = adc.snapshot_id
left join {{ source('activite_dsci', 'ref_direction') }} as adrd
    on adrd.id = adc.id_direction
    and adrd.snapshot_id = adc.snapshot_id
left join cte_region as ctereg
    on ctereg.id = adc.id_region
    and ctereg.snapshot_id = adc.snapshot_id
