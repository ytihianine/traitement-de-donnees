with
    liste_biens_oad_osfi as (
        -- Liste complète des biens entre l'OAD et l'OSFI par snapshot_id
        select distinct
            code_bat_ter,
            code_gestionnaire,
            code_bat_gestionnaire,
            snapshot_id
        from {{ source('siep', 'bien_gestionnaire') }}

        union

        select distinct
            code_bat_ter,
            code_gestionnaire,
            code_bat_gestionnaire,
            snapshot_id_parent
        from {{ source('siep', 'bien_information_complementaire') }}
    ),
    cte_bien_gest_oad_osfi as (
        select
            lboo.code_bat_ter,
            lboo.code_gestionnaire,
            lboo.code_bat_gestionnaire,
            lboo.snapshot_id,
            vs.import_timestamp as oad_import_timestamp,
            vs_parent.import_timestamp as osfi_import_timestamp
        from liste_biens_oad_osfi as lboo
        left join {{ source('versioning', 'snapshot') }} as vs
            on vs.snapshot_id = lboo.snapshot_id
            and vs.is_dag_completed is true
        left join {{ source('versioning', 'snapshot') }} as vs_parent
            on vs_parent.snapshot_id_parent = lboo.snapshot_id
            and vs_parent.is_dag_completed is true
    ),
    cte_bien_occupant_agrege as (
        -- Agrégation des indicateurs à la maille code_bat_gestionnaire (bien gestionnaire)
        select
            sbo.code_bat_gestionnaire,
            sbo.snapshot_id,
            sbo.import_timestamp,
            sum(sbo.indicateur_sub_occ) as sum_indicateur_sub_occ,
            sum(sbo.indicateur_surface_mef_occ) as sum_indicateur_surface_mef_occ,
            sum(sbo.indicateur_poste_occ) as sum_indicateur_poste_occ,
            sum(sbo.indicateur_resident_occ) as sum_indicateur_resident_occ,
            sum(sbo.indicateur_resident_reconstitue_occ) as sum_indicateur_resident_reconstitue_occ,
            sum(sbo.indicateur_sub_occ_source) as sum_indicateur_sub_occ_source,
            sum(sbo.indicateur_poste_occ_source) as sum_indicateur_poste_occ_source,
            sum(sbo.indicateur_resident_occ_source) as sum_indicateur_resident_occ_source
        from {{ source('siep', 'bien_occupant') }} as sbo
        group by sbo.snapshot_id, sbo.import_timestamp, sbo.code_bat_gestionnaire
    ),
    -- CTE pour consolider l'état au niveau code_bat_ter
    cte_etat_bat_consolide as (
        select
            sbic.snapshot_id,
            sbic.import_timestamp,
            sbic.code_bat_ter,
            -- Compte les occurrences avec état "ouvert"
            count(case when sbic.etat_bat = 'Ouvert' then 1 end) as nb_ouvert,
            -- Compte les occurrences avec état "fermé"
            count(case when sbic.etat_bat = 'Fermé' then 1 end) as nb_ferme,
            -- Compte les occurrences avec état différent de N/A
            count(case when sbic.etat_bat is not null then 1 end) as nb_etat_non_null,
            -- Vérifie si le bâtiment existe dans l'OAD
            max(case
                when exists (
                    select 1
                    from {{ source('siep', 'bien') }} as sb
                    where sb.snapshot_id = sbic.snapshot_id
                      and sb.code_bat_ter = sbic.code_bat_ter
                ) then 1
                else 0
            end) as existe_dans_oad
        from {{ source('siep', 'bien_information_complementaire') }} as sbic
        where sbic.code_bat_ter is not null
        group by sbic.snapshot_id, sbic.import_timestamp, sbic.code_bat_ter
    ),
    cte_etat_bat_final as (
        select
            snapshot_id,
            import_timestamp,
            code_bat_ter,
            case
                -- Si au moins un gestionnaire marque le bâtiment comme "ouvert"
                when nb_ouvert > 0 then 'Ouvert'
                -- Si aucun ouvert mais au moins un fermé
                when nb_ouvert = 0 and nb_ferme > 0 then 'Fermé'
                -- Si tous les états sont N/A
                when nb_etat_non_null = 0 then
                    case
                        when existe_dans_oad = 1 then 'Ouvert'
                        else 'Fermé'
                    end
                -- Cas par défaut : prendre un état non N/A s'il existe
                else 'Indéterminé'
            end as etat_bat_recalcule
        from cte_etat_bat_consolide
    )
select
    -- bien_gestionnaire issues de l'OAD et OSFI
    cte_bgoo.code_bat_ter,
    cte_bgoo.code_bat_gestionnaire,
    cte_bgoo.code_gestionnaire,
    cte_bgoo.snapshot_id,
    -- siep.bien sb && siep.bien_information_complementaire sbic
    coalesce(sb.code_site, sbic.code_site) as code_site,
    sb.categorie_administrative_liste_bat as categorie_administrative_liste_bat,
    sb.categorie_administrative_principale_bat as categorie_administrative_principale_bat,
    sb.date_construction_annee_corrigee as date_construction_annee_corrigee,
    sb.gestionnaire_type_simplifie_bat as gestionnaire_type_simplifie_bat,
    sb.gestionnaire_presents_liste_mef as gestionnaire_presents_liste_mef,
    sb.gest_princ_personnalite_juridique as gest_princ_personnalite_juridique,
    sb.gest_princ_personnalite_juridique_simplifiee as gest_princ_personnalite_juridique_simplifiee,
    sb.gest_princ_personnalite_juridique_precision as gest_princ_personnalite_juridique_precision,
    sb.gestionnaire_principal_code as gestionnaire_principal_code,
    sb.gestionnaire_principal_libelle as gestionnaire_principal_libelle,
    sb.gestionnaire_principal_libelle_simplifie as gestionnaire_principal_libelle_simplifie,
    sb.gestionnaire_principal_lien_mef as gestionnaire_principal_lien_mef,
    sb.gestionnaire_principal_ministere as gestionnaire_principal_ministere,
    sb.libelle_bat_ter as libelle_bat_ter,
    coalesce(sb.gestion_mono_multi_min, sbic.gestion_mono_multi_min) as gestion_mono_multi_min,
    coalesce(sb.gestion_mono_multi_mef, sbic.gestion_mono_multi_mef) as gestion_mono_multi_mef,
    coalesce(sb.gestion_categorie, sbic.gestion_categorie) as gestion_categorie,
    -- siep.gestionnaire
    sgest.ministere as ministere,
    sgest.libelle_gestionnaire as libelle_gestionnaire,
    coalesce(sgest.libelle_simplifie, 'Indéterminé') as libelle_simplifie,
    sgest.libelle_abrege as libelle_abrege,
    sgest.lien_mef_gestionnaire as lien_mef_gestionnaire,
    sgest.personnalite_juridique as gestionnaire_personnalite_juridique,
    sgest.personnalite_juridique_precision as gestionnaire_personnalite_juridique_precision,
    sgest.personnalite_juridique_simplifiee as gestionnaire_personnalite_juridique_simplifie,
    -- siep.bien_strategie sbs
    sbs.perimetre_spsi_initial as perimetre_spsi_initial,
    sbs.perimetre_spsi_maj as perimetre_spsi_maj,
    -- siep.bien_deet_energie_ges sbdeg && siep.bien_information_complementaire sbic
    sbdeg.bat_assujettis_deet as bat_assujettis_deet_oad,
    sbic.bat_assujettis_deet as bat_assujettis_deet_osfi,
    -- siep.bien_proprietaire sbp
    sbp.locatif_domanial as locatif_domanial,
    -- siep.bien_reglementation sbr
    sbr.reglementation_corrigee as reglementation_corrigee,
    -- siep.bien_typologie sbt && siep.bien_information_complementaire sbic
    coalesce(sbt.usage_detaille_du_bien, sbic.usage_detaille_du_bien, 'Indéterminé') as usage_detaille_du_bien,
    -- siep.ref_typologie srt
    srt.bati_non_bati as bati_non_bati,
    srt.type_de_bien as type_de_bien,
    srt.famille_de_bien as famille_de_bien,
    srt.famille_de_bien_simplifiee as famille_de_bien_simplifiee,
    -- cte_bien_occupant_agrege cte_boa && siep.bien_information_complementaire sbic
    cte_boa.sum_indicateur_sub_occ as sum_indicateur_sub_occ,
    cte_boa.sum_indicateur_surface_mef_occ as sum_indicateur_surface_mef_occ,
    coalesce(cte_boa.sum_indicateur_poste_occ, sbic.indicateur_poste_occ) as sum_indicateur_poste_occ,
    coalesce(cte_boa.sum_indicateur_resident_occ, sbic.indicateur_resident_occ) as sum_indicateur_resident_occ,
    cte_boa.sum_indicateur_resident_reconstitue_occ as sum_indicateur_resident_reconstitue_occ,
    coalesce(cte_boa.sum_indicateur_sub_occ_source, sbic.indicateur_sub_occ_source) as sum_indicateur_sub_occ_source,
    coalesce(cte_boa.sum_indicateur_poste_occ_source, sbic.indicateur_poste_occ_source) as sum_indicateur_poste_occ_source,
    cte_boa.sum_indicateur_resident_occ_source as sum_indicateur_resident_occ_source,
    -- siep.bien_information_complementaire sbic && cte_etat_bat_final cte_ebf
    cte_ebf.etat_bat_recalcule as etat_bat_recalcule,
    sbic.date_sortie_bat as date_sortie_bat,
    sbic.date_sortie_site as date_sortie_site,
    sbic.date_derniere_renovation as date_derniere_renovation,
    sbic.annee_reference as annee_reference,
    sbic.efa as efa,
    -- Date d'import
    cte_bgoo.oad_import_timestamp,
    cte_bgoo.osfi_import_timestamp
from cte_bien_gest_oad_osfi as cte_bgoo
-- Jointures datasets issus de l'OAD
left join {{ source('siep', 'bien') }} as sb
    on sb.import_timestamp = cte_bgoo.oad_import_timestamp
    and sb.code_bat_ter = cte_bgoo.code_bat_ter
left join {{ source('siep', 'gestionnaire') }} as sgest
    on sgest.import_timestamp = cte_bgoo.oad_import_timestamp
    and sgest.code_gestionnaire = cte_bgoo.code_gestionnaire
left join {{ source('siep', 'bien_strategie') }} as sbs
    on sbs.import_timestamp = cte_bgoo.oad_import_timestamp
    and sbs.code_bat_ter = cte_bgoo.code_bat_ter
left join {{ source('siep', 'bien_deet_energie_ges') }} as sbdeg
    on sbdeg.import_timestamp = cte_bgoo.oad_import_timestamp
    and sbdeg.code_bat_ter = cte_bgoo.code_bat_ter
left join {{ source('siep', 'bien_proprietaire') }} as sbp
    on sbp.import_timestamp = cte_bgoo.oad_import_timestamp
    and sbp.code_bat_ter = cte_bgoo.code_bat_ter
left join {{ source('siep', 'bien_reglementation') }} as sbr
    on sbr.import_timestamp = cte_bgoo.oad_import_timestamp
    and sbr.code_bat_ter = cte_bgoo.code_bat_ter
left join {{ source('siep', 'bien_typologie') }} as sbt
    on sbt.import_timestamp = cte_bgoo.oad_import_timestamp
    and sbt.code_bat_ter = cte_bgoo.code_bat_ter
-- Jointures datasets issus de l'OSFI
left join {{ source('siep', 'bien_information_complementaire') }} as sbic
    on sbic.import_timestamp = cte_bgoo.osfi_import_timestamp
    and sbic.code_bat_gestionnaire = cte_bgoo.code_bat_gestionnaire
-- Jointure avec ref_typologie basée sur usage_detaille_du_bien coalescé
left join {{ source('siep', 'ref_typologie') }} as srt
    on srt.usage_detaille_du_bien = coalesce(sbt.usage_detaille_du_bien, sbic.usage_detaille_du_bien)
-- Jointure avec cte_bien_occupant_agrege
left join cte_bien_occupant_agrege as cte_boa
    on cte_boa.import_timestamp = cte_bgoo.oad_import_timestamp
    and cte_boa.code_bat_gestionnaire = cte_bgoo.code_bat_gestionnaire
-- Jointure avec l'état recalculé au niveau code_bat_ter
left join cte_etat_bat_final as cte_ebf
    on cte_ebf.import_timestamp = cte_bgoo.osfi_import_timestamp
    and cte_ebf.code_bat_ter = cte_bgoo.code_bat_ter
