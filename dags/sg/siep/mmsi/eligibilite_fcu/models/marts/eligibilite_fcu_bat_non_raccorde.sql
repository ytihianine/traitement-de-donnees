
{{ config(materialized='view') }}

with cte_conso_corr_agregee as (
    select
        sbccg.code_bat_ter,
        sbccg.gestionnaire_principal_libelle_simplifie,
        scau.snapshot_id,
        scau.import_timestamp,
        sum(case when scau.fluide in ('gaz_pci') then scau.conso else 0 end) as conso_corr_gazpci,
        sum(case when scau.fluide in ('fioul') then scau.conso else 0 end) as conso_corr_fioul,
        sum(case when scau.fluide in ('gaz_pci', 'fioul') then scau.conso else 0 end) as conso_corr_gazpci_fioul,
        sum(case when scau.fluide in ('RCU') then scau.conso else 0 end) as conso_corr_rcu
    from {{ source('siep', 'conso_annuelle_unpivot') }} as scau
    left join {{ source('siep', 'bien_caracteristiques_complet_gestionnaire_vw') }} as sbccg
        on scau.import_timestamp = sbccg.osfi_import_timestamp
        and scau.code_bat_gestionnaire = sbccg.code_bat_gestionnaire
    where scau.type_conso = 'CORRIGEE'
    group by scau.snapshot_id, scau.import_timestamp, sbccg.code_bat_ter, sbccg.gestionnaire_principal_libelle_simplifie
)

select
    cte_cca.code_bat_ter,
    cte_cca.gestionnaire_principal_libelle_simplifie,
    cte_cca.conso_corr_gazpci_fioul,
    cte_cca.conso_corr_rcu,
    sbef.is_eligible,
    sbef.distance,
    sbef.distance_rattachement,
    sbef.in_pdp,
    sbef.pdp,
    sbef.futur_network,
    sbef.rcu_etat,
    sbef.id_fcu,
    sbef.name,
    sbef.gestionnaire_fcu,
    sbef.taux_enrr_rcu,
    sbef.rate_co2,
    sbef.contenu_rcu_gco2,
    sbef.api_status,
    sbef.api_status_code,
    sbef.api_raison,
    sbef.import_timestamp,
    sbef.import_date,
    sbef.snapshot_id,
    sbef.import_timestamp_oad,
    round(
        cast((
            (coalesce(cte_cca.conso_corr_fioul, 0) * 0.324)
            + (coalesce(cte_cca.conso_corr_gazpci, 0) * 0.227)
            - ((coalesce(cte_cca.conso_corr_fioul, 0) + coalesce(cte_cca.conso_corr_gazpci, 0)) * coalesce(sbef.rate_co2, 0))
        ) / 1000000 as numeric), 2
    ) as eges_ktco2_evitees
from {{ source('siep', 'bien_eligibilite_fcu') }} as sbef
left join cte_conso_corr_agregee as cte_cca
    on cte_cca.import_timestamp = sbef.import_timestamp
    and cte_cca.code_bat_ter = sbef.code_bat_ter
where cte_cca.conso_corr_rcu = 0
