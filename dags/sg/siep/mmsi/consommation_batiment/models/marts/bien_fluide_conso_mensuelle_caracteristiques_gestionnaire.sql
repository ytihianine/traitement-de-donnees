
{{ config(materialized='view') }}

select
    scm.code_bat_gestionnaire,
    scm.date_conso as date_conso,
    date_part('year', scm.date_conso) as annee,
    date_part('month', scm.date_conso) as mois,
    scm.conso_elec as conso_elec,
    scm.conso_elec_corr_dju_mmsi as conso_elec_corr_dju_mmsi,
    scm.conso_gaz_pci as conso_gaz_pci,
    scm.conso_gaz_pci_corr_dju_mmsi as conso_gaz_pci_corr_dju_mmsi,
    scm.conso_reseau_chaleur as conso_reseau_chaleur,
    scm.conso_reseau_chaleur_corr_dju_mmsi as conso_reseau_chaleur_corr_dju_mmsi,
    scm.conso_reseau_froid as conso_reseau_froid,
    scm.conso_reseau_froid_corr_dju_mmsi as conso_reseau_froid_corr_dju_mmsi,
    scm.conso_fioul_pci as conso_fioul_pci,
    scm.conso_fioul_pci_corr_dju_mmsi as conso_fioul_pci_corr_dju_mmsi,
    scm.import_timestamp as scm_import_timestamp,
    (
        COALESCE(conso_elec, 0) +
        COALESCE(conso_gaz_pci, 0) +
        COALESCE(conso_reseau_chaleur, 0) +
        COALESCE(conso_reseau_froid, 0) +
        COALESCE(conso_fioul_pci, 0)
    ) as conso_fluides,
    (
        COALESCE(conso_elec_corr_dju_mmsi, 0) +
        COALESCE(conso_gaz_pci_corr_dju_mmsi, 0) +
        COALESCE(conso_reseau_chaleur_corr_dju_mmsi, 0) +
        COALESCE(conso_reseau_froid_corr_dju_mmsi, 0) +
        COALESCE(conso_fioul_pci_corr_dju_mmsi, 0)
    ) as conso_fluides_corr_dju_mmsi,
    (
        COALESCE(conso_reseau_chaleur_corr_dju_mmsi, 0) +
        COALESCE(conso_reseau_froid_corr_dju_mmsi, 0) +
        COALESCE(conso_fioul_pci_corr_dju_mmsi, 0)
    ) as conso_autres_fluides_corr_dju_mmsi,
    sbccg.snapshot_id,
    sbccg.oad_import_timestamp,
    sbccg.osfi_import_timestamp,
    sbccg.code_gestionnaire,
    sbccg.code_site,
    sbccg.code_bat_ter,
    sbccg.categorie_administrative_liste_bat,
    sbccg.categorie_administrative_principale_bat,
    sbccg.date_construction_annee_corrigee,
    sbccg.gestionnaire_type_simplifie_bat,
    sbccg.gestionnaire_presents_liste_mef,
    sbccg.gest_princ_personnalite_juridique,
    sbccg.gest_princ_personnalite_juridique_simplifiee,
    sbccg.gest_princ_personnalite_juridique_precision,
    sbccg.gestionnaire_principal_code,
    sbccg.gestionnaire_principal_libelle,
    sbccg.gestionnaire_principal_libelle_simplifie,
    sbccg.gestionnaire_principal_lien_mef,
    sbccg.gestionnaire_principal_ministere,
    sbccg.libelle_bat_ter,
    sbccg.gestion_mono_multi_mef,
    sbccg.gestion_categorie,
    sbccg.gestion_mono_multi_min,
    sbccg.etat_bat_recalcule,
    sbccg.date_sortie_bat,
    sbccg.date_sortie_site,
    sbccg.date_derniere_renovation,
    sbccg.annee_reference,
    sbccg.efa,
    sbccg.libelle_gestionnaire,
    sbccg.ministere,
    sbccg.libelle_simplifie,
    sbccg.libelle_abrege,
    sbccg.lien_mef_gestionnaire,
    sbccg.gestionnaire_personnalite_juridique,
    sbccg.gestionnaire_personnalite_juridique_precision,
    sbccg.gestionnaire_personnalite_juridique_simplifie,
    sbccg.bat_assujettis_deet_oad,
    sbccg.bat_assujettis_deet_osfi,
    sbccg.perimetre_spsi_initial,
    sbccg.perimetre_spsi_maj,
    sbccg.locatif_domanial,
    sbccg.reglementation_corrigee,
    sbccg.usage_detaille_du_bien,
    sbccg.bati_non_bati,
    sbccg.type_de_bien,
    sbccg.famille_de_bien,
    sbccg.famille_de_bien_simplifiee,
    sbccg.sum_indicateur_sub_occ,
    sbccg.sum_indicateur_surface_mef_occ,
    sbccg.sum_indicateur_poste_occ,
    sbccg.sum_indicateur_resident_occ,
    sbccg.sum_indicateur_resident_reconstitue_occ,
    sbccg.sum_indicateur_sub_occ_source,
    sbccg.sum_indicateur_poste_occ_source,
    sbccg.sum_indicateur_resident_occ_source,
    sbccg.oad_import_timestamp,
    sbccg.osfi_import_timestamp
from {{ source('siep', 'conso_mensuelle') }} as scm
left join {{ source('siep', 'bien_caracteristiques_complet_gestionnaire_vw') }} as sbccg
    on sbccg.code_bat_gestionnaire = scm.code_bat_gestionnaire
    and sbccg.osfi_import_timestamp = scm.import_timestamp
