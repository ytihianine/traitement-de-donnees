
select
    dim_bgc.code_bat_ter as code_bat_ter,
    dim_bgc.libelle_bat_ter as libelle_bat_ter,
    dim_bgc.code_gestionnaire as code_gestionnaire,
    dim_bgc.gestion_categorie as gestion_categorie,
    dim_bgc.gestionnaire_principal_libelle_simplifie as gestionnaire_principal_libelle_simplifie,
    dim_bgc.gestionnaire_principal_libelle as gestionnaire_principal_libelle,
    dim_bgc.gestionnaire_principal_lien_mef as gestionnaire_principal_lien_mef,
    dim_bgc.gestionnaire_personnalite_juridique_simplifie as gestionnaire_personnalite_juridique_simplifie,
    dim_bgc.gestionnaire_personnalite_juridique_precision as gestionnaire_personnalite_juridique_precision,
    dim_bgc.gestionnaire_personnalite_juridique as gestionnaire_personnalite_juridique,
    dim_bgc.gestionnaire_principal_ministere as gestionnaire_principal_ministere,
    dim_bgc.famille_de_bien as famille_de_bien,
    dim_bgc.type_de_bien as type_de_bien,
    dim_bgc.usage_detaille_du_bien as usage_detaille_du_bien,
    dim_bgc.categorie_administrative_principale_bat as categorie_administrative_principale_bat,
    dim_bgc.sum_indicateur_surface_mef_occ as sum_indicateur_surface_mef_occ,
    dim_bgc.oad_import_timestamp as oad_import_timestamp,
    sbgeo.risque_categorie as risque_categorie,
    sbgeo.risque_libelle as risque_libelle,
    sbgeo.risque_present as risque_present,
    sbgeo.import_timestamp as import_timestamp,
    sbeds.composant_bien as eds_composant_bien,
    sbeds.eds_theorique as eds_theorique,
    sbeds.eds_constate as eds_constate,
    sbad.composant_bien as acc_detail_composant_bien,
    sbad.niveau as acc_detail_niveau
from {{ ref('dim_bien_gestionnaire_complet') }} as dim_bgc
left join {{ source('siep', 'bien_georisque') }} as sbgeo
    on dim_bgc.snapshot_id = sbgeo.snapshot_id_parent
    and dim_bgc.code_bat_ter = sbgeo.code_bat_ter
left join {{ source('siep', 'bien_etat_de_sante') }} as sbeds
    on dim_bgc.oad_import_timestamp = sbeds.import_timestamp
    and dim_bgc.code_bat_ter = sbeds.code_bat_ter
left join {{ source('siep', 'bien_accessibilite_detail') }} as sbad
    on dim_bgc.oad_import_timestamp = sbad.import_timestamp
    and dim_bgc.code_bat_ter = sbad.code_bat_ter
