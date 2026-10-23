

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
    dim_bgc.snapshot_id,
    dim_bgc.oad_import_timestamp,
    dim_bgc.osfi_import_timestamp,
    dim_bgc.code_gestionnaire,
    dim_bgc.code_site,
    dim_bgc.code_bat_ter,
    dim_bgc.categorie_administrative_liste_bat,
    dim_bgc.categorie_administrative_principale_bat,
    dim_bgc.date_construction_annee_corrigee,
    dim_bgc.gestionnaire_type_simplifie_bat,
    dim_bgc.gestionnaire_presents_liste_mef,
    dim_bgc.gest_princ_personnalite_juridique,
    dim_bgc.gest_princ_personnalite_juridique_simplifiee,
    dim_bgc.gest_princ_personnalite_juridique_precision,
    dim_bgc.gestionnaire_principal_code,
    dim_bgc.gestionnaire_principal_libelle,
    dim_bgc.gestionnaire_principal_libelle_simplifie,
    dim_bgc.gestionnaire_principal_lien_mef,
    dim_bgc.gestionnaire_principal_ministere,
    dim_bgc.libelle_bat_ter,
    dim_bgc.gestion_mono_multi_mef,
    dim_bgc.gestion_categorie,
    dim_bgc.gestion_mono_multi_min,
    dim_bgc.etat_bat_recalcule,
    dim_bgc.date_sortie_bat,
    dim_bgc.date_sortie_site,
    dim_bgc.date_derniere_renovation,
    dim_bgc.annee_reference,
    dim_bgc.efa,
    dim_bgc.libelle_gestionnaire,
    dim_bgc.ministere,
    dim_bgc.libelle_simplifie,
    dim_bgc.libelle_abrege,
    dim_bgc.lien_mef_gestionnaire,
    dim_bgc.gestionnaire_personnalite_juridique,
    dim_bgc.gestionnaire_personnalite_juridique_precision,
    dim_bgc.gestionnaire_personnalite_juridique_simplifie,
    dim_bgc.bat_assujettis_deet_oad,
    dim_bgc.bat_assujettis_deet_osfi,
    dim_bgc.perimetre_spsi_initial,
    dim_bgc.perimetre_spsi_maj,
    dim_bgc.locatif_domanial,
    dim_bgc.reglementation_corrigee,
    dim_bgc.usage_detaille_du_bien,
    dim_bgc.bati_non_bati,
    dim_bgc.type_de_bien,
    dim_bgc.famille_de_bien,
    dim_bgc.famille_de_bien_simplifiee,
    dim_bgc.sum_indicateur_sub_occ,
    dim_bgc.sum_indicateur_surface_mef_occ,
    dim_bgc.sum_indicateur_poste_occ,
    dim_bgc.sum_indicateur_resident_occ,
    dim_bgc.sum_indicateur_resident_reconstitue_occ,
    dim_bgc.sum_indicateur_sub_occ_source,
    dim_bgc.sum_indicateur_poste_occ_source,
    dim_bgc.sum_indicateur_resident_occ_source,
    dim_bgc.oad_import_timestamp,
    dim_bgc.osfi_import_timestamp
from {{ source('siep', 'conso_mensuelle') }} as scm
left join from {{ ref('dim_bien_gestionnaire_complet') }} as dim_bgc
    on dim_bgc.code_bat_gestionnaire = scm.code_bat_gestionnaire
    and dim_bgc.osfi_import_timestamp = scm.import_timestamp
