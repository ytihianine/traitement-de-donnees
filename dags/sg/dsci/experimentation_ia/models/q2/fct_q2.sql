
select
    q2.id_row,
    q2.id,
    q2.no_id,
    q2.autres_types_d_interactions,
    q2.frequence_d_usage_assistant_ia,
    q2.autres_formation_ia,
    q2.raison_non_participation_rdv,
    q2.autre_besoin_accompagnement,
    q2.apprentissage_assistant_ia_ressenti_,
    q2.difficultes_techniques_rencontrees2,
    q2.autres_difficultes,
    q2.autres_freins,
    q2.autres_sources_de_progression,
    q2.autres_taches_realisees,
    q2.decouverte_d_usages_inattendus,
    q2.les_usages_inattendus,
    q2.mode_de_decouverte_usages,
    q2.autre_mode_de_decouverte,
    q2.frequence_des_erreurs,
    q2.autres_types_d_erreurs,
    q2.cas_usage_principal_teste,
    q2.cu1_temps_economise_par_semaine,
    q2.cu1_nombre_echanges_moyens_affinage_reponse,
    q2.cu1_pertinence_assistant_ia,
    q2.cu1_commentaires,
    q2.deuxieme_cas_d_usage_teste,
    q2.cu2_temps_economise_par_semaine,
    q2.cu2_nombre_echanges_moyens,
    q2.cu2_pertinence_assistant_ia,
    q2.cu2_commentaires,
    q2.troisieme_cas_d_usage,
    q2.cu3_temps_economise_par_semaine,
    q2.cu3_nombre_echanges_moyens_affinage_reponse,
    q2.cu3_pertinence_assistant_ia,
    q2.cu3_commentaires,
    q2.autres_impacts_identifies,
    q2.autres_impacts_observes,
    q2.impact_sur_le_temps_de_travail,
    q2.estimation_globale_gain_de_temps,
    q2.autres_raisons,
    q2.id_niveau_d_usage_ia_post_expe_,
    niveau_appropriation.niveau_appropriation_libelle_court as niveau_d_usage_ia_post_expe,
    q2.id_recommandation_collegues_mef,
    accord_recommandation.niveau_accord as recommandation_collegues_mef,
    q2.id_sensation_montee_en_competences,
    accord_sensation.niveau_accord as sensation_montee_en_competences,
    q2.id_evolution_des_craintes_initiales,
    evolution_crainte.evolutions as evolution_des_craintes_initiales,
    q2.id_utilite_metier_mef,
    accord_utilite.niveau_accord as utilite_metier_mef,
    q2.id_diminution_d_usage_ia_non_souveraines,
    accord_diminution.niveau_accord as diminution_d_usage_ia_non_souveraines,
    q2.id_comparaison_autres_ia,
    comparaison_autre_ia.comparaisons as comparaison_autres_ia,
    q2.id_cu1_taux_moyen_de_correction_rep_assistant,
    taux_correction_cu1.taux_de_correction as cu1_taux_moyen_de_correction_rep_assistant,
    taux_correction_cu1.taux_correction_simplifie as cu1_taux_moyen_correction_simplifie,
    q2.id_cu2_taux_moyen_de_correction_rep_assistant,
    taux_correction_cu2.taux_de_correction as cu2_taux_moyen_de_correction_rep_assistant,
    taux_correction_cu2.taux_correction_simplifie as cu2_taux_moyen_correction_simplifie,
    q2.id_cu3_taux_moyen_de_correction_rep_assistant,
    taux_correction_cu3.taux_de_correction as cu3_taux_moyen_de_correction_rep_assistant,
    taux_correction_cu3.taux_correction_simplifie as cu3_taux_moyen_correction_simplifie,
    q2.id_raisons_perte_de_temps,
    raison_perte_temps.raisons as raisons_perte_de_temps,
    q2.id_ia_favorise_relations_humaines_,
    accord_humaines.niveau_accord as ia_favorise_relations_humaines,
    q2.import_timestamp,
    q2.snapshot_id,
    q2.snapshot_id_parent
from {{ source('assistant_ia', 'q2') }} as q2
left join {{ source('assistant_ia', 'ref_niveau_appropriation') }} as niveau_appropriation
    on q2.id_niveau_d_usage_ia_post_expe_ = niveau_appropriation.id
left join {{ source('assistant_ia', 'ref_niveau_accord') }} as accord_recommandation
    on q2.id_recommandation_collegues_mef = accord_recommandation.id
left join {{ source('assistant_ia', 'ref_niveau_accord') }} as accord_sensation
    on q2.id_sensation_montee_en_competences = accord_sensation.id
left join {{ source('assistant_ia', 'ref_evolution_crainte') }} as evolution_crainte
    on q2.id_evolution_des_craintes_initiales = evolution_crainte.id
left join {{ source('assistant_ia', 'ref_niveau_accord') }} as accord_utilite
    on q2.id_utilite_metier_mef = accord_utilite.id
left join {{ source('assistant_ia', 'ref_niveau_accord') }} as accord_diminution
    on q2.id_diminution_d_usage_ia_non_souveraines = accord_diminution.id
left join {{ source('assistant_ia', 'ref_comparaison_autre_ia') }} as comparaison_autre_ia
    on q2.id_comparaison_autres_ia = comparaison_autre_ia.id
left join {{ source('assistant_ia', 'ref_taux_correction') }} as taux_correction_cu1
    on q2.id_cu1_taux_moyen_de_correction_rep_assistant = taux_correction_cu1.id
left join {{ source('assistant_ia', 'ref_taux_correction') }} as taux_correction_cu2
    on q2.id_cu2_taux_moyen_de_correction_rep_assistant = taux_correction_cu2.id
left join {{ source('assistant_ia', 'ref_taux_correction') }} as taux_correction_cu3
    on q2.id_cu3_taux_moyen_de_correction_rep_assistant = taux_correction_cu3.id
left join {{ source('assistant_ia', 'ref_raison_perte_temps') }} as raison_perte_temps
    on q2.id_raisons_perte_de_temps = raison_perte_temps.id
left join {{ source('assistant_ia', 'ref_niveau_accord') }} as accord_humaines
    on q2.id_ia_favorise_relations_humaines_ = accord_humaines.id
order by
    q2.no_id,
    q2.id_row,
    niveau_appropriation.id_row,
    accord_recommandation.id_row,
    accord_sensation.id_row,
    evolution_crainte.id_row,
    accord_utilite.id_row,
    accord_diminution.id_row,
    comparaison_autre_ia.id_row,
    taux_correction_cu1.id_row,
    taux_correction_cu2.id_row,
    taux_correction_cu3.id_row,
    raison_perte_temps.id_row,
    accord_humaines.id_row
