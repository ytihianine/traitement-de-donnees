
{{ config(materialized='view') }}

select
    sbccg.code_bat_ter as code_bat_ter,
    sbccg.libelle_bat_ter as libelle_bat_ter,
    sbccg.code_gestionnaire as code_gestionnaire,
    sbccg.gestion_categorie as gestion_categorie,
    sbccg.gestionnaire_principal_libelle_simplifie as gestionnaire_principal_libelle_simplifie,
    sbccg.gestionnaire_principal_libelle as gestionnaire_principal_libelle,
    sbccg.gestionnaire_principal_lien_mef as gestionnaire_principal_lien_mef,
    sbccg.gestionnaire_personnalite_juridique_simplifie as gestionnaire_personnalite_juridique_simplifie,
    sbccg.gestionnaire_personnalite_juridique_precision as gestionnaire_personnalite_juridique_precision,
    sbccg.gestionnaire_personnalite_juridique as gestionnaire_personnalite_juridique,
    sbccg.gestionnaire_principal_ministere as gestionnaire_principal_ministere,
    sbccg.famille_de_bien as famille_de_bien,
    sbccg.type_de_bien as type_de_bien,
    sbccg.usage_detaille_du_bien as usage_detaille_du_bien,
    sbccg.categorie_administrative_principale_bat as categorie_administrative_principale_bat,
    sbccg.sum_indicateur_surface_mef_occ as sum_indicateur_surface_mef_occ,
    sbccg.oad_import_timestamp as oad_import_timestamp,
    sbgeo.risque_categorie as risque_categorie,
    sbgeo.risque_libelle as risque_libelle,
    sbgeo.risque_present as risque_present,
    sbgeo.import_timestamp as import_timestamp,
    sbeds.composant_bien as eds_composant_bien,
    sbeds.eds_theorique as eds_theorique,
    sbeds.eds_constate as eds_constate,
    sbad.composant_bien as acc_detail_composant_bien,
    sbad.niveau as acc_detail_niveau
from {{ source('siep', 'bien_caracteristiques_complet_gestionnaire_vw') }} as sbccg
left join {{ source('siep', 'bien_georisque') }} as sbgeo
    on sbccg.snapshot_id = sbgeo.snapshot_id_parent
    and sbccg.code_bat_ter = sbgeo.code_bat_ter
left join {{ source('siep', 'bien_etat_de_sante') }} as sbeds
    on sbccg.oad_import_timestamp = sbeds.import_timestamp
    and sbccg.code_bat_ter = sbeds.code_bat_ter
left join {{ source('siep', 'bien_accessibilite_detail') }} as sbad
    on sbccg.oad_import_timestamp = sbad.import_timestamp
    and sbccg.code_bat_ter = sbad.code_bat_ter
