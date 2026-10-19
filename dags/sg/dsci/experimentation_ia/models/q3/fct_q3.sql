
select
    q3.id_row,
    q3.id,
    q3.no_id,
    q3.temps_fonction_exercee,
    q3.genre,
    q3.frequence_utilisation,
    q3.evolution_usage,
    q3.quelles_raisons_facons,
    q3.autres_outils,
    q3.satisfaction_autre_outil,
    q3.autres,
    q3.evaluation_niveau_acculturation,
    q3.evolution_sentiment,
    q3.autres_leviers,
    q3.temps_gagnes,
    q3.impact_perception,
    q3.sentiment_de_fierte,
    q3.experimentation_interne,
    q3.autres_fonctionnalites,
    q3.autres_risques,
    q3.etre_ambassadeur,
    q3.bonnes_pratiques,
    q3.autres_besoins_importants,
    q3.autres_besoins_moindres,
    q3.ameliorations,
    q3.aspects_a_ameliorer,
    q3.interface,
    q3.contenu,
    q3.connexions,
    q3.autre_retour_libre,
    q3.retours_libres,
    q3.id_raisons_non_participation,
    raison_non_participation.raisons as raisons_non_participation,
    q3.id_impacts_taches_pro,
    impact_tache_pro.impacts as impacts_taches_pro,
    q3.id_impacts_taches_rebarbatives,
    impact_tache_rebarbative.taches_rebarbatives as impacts_taches_rebarbatives,
    q3.id_comparaison_autres_ia,
    comparaison_autre_ia.comparaisons as comparaison_autres_ia,
    q3.id_utilisation_moindre,
    accord_utilisation.niveau_accord as utilisation_moindre,
    q3.id_recommandations,
    accord_recommandations.niveau_accord as recommandations,
    q3.import_timestamp,
    q3.snapshot_id,
    q3.snapshot_id_parent
from {{ source('assistant_ia', 'q3') }} as q3
left join {{ source('assistant_ia', 'ref_raison_non_participation') }} as raison_non_participation
    on q3.id_raisons_non_participation = raison_non_participation.id
left join {{ source('assistant_ia', 'ref_impact_tache_pro') }} as impact_tache_pro
    on q3.id_impacts_taches_pro = impact_tache_pro.id
left join {{ source('assistant_ia', 'ref_impact_tache_rebarbative') }} as impact_tache_rebarbative
    on q3.id_impacts_taches_rebarbatives = impact_tache_rebarbative.id
left join {{ source('assistant_ia', 'ref_comparaison_autre_ia') }} as comparaison_autre_ia
    on q3.id_comparaison_autres_ia = comparaison_autre_ia.id
left join {{ source('assistant_ia', 'ref_niveau_accord') }} as accord_utilisation
    on q3.id_utilisation_moindre = accord_utilisation.id
left join {{ source('assistant_ia', 'ref_niveau_accord') }} as accord_recommandations
    on q3.id_recommandations = accord_recommandations.id
order by
    q3.no_id,
    q3.id_row,
    raison_non_participation.id_row,
    impact_tache_pro.id_row,
    impact_tache_rebarbative.id_row,
    comparaison_autre_ia.id_row,
    accord_utilisation.id_row,
    accord_recommandations.id_row
