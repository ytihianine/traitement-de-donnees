
select
    e.id_row,
    e.id,
    e.no_id,
    e.entite,
    e.connecte_,
    e.reponse_au_questionnaire_1,
    e.reponse_au_questionnaire_2,
    e.reponse_au_questionnaire_3,
    e.parti,
    q1.id_direction,
    direction.direction,
    direction.direction_regroupee,
    q1.tranche_age,
    q1.categorie_emploi,
    q1.statut,
    q1.id_domaine_professionnel,
    domaine_professionnel.domaine as domaine_professionnel,
    q1.situation_d_encadrement,
    q1.autres_experimentateurs,
    q1.id_niveau_d_utilisation_ia,
    niveau_appropriation.niveau_appropriation_libelle_court as niveau_d_utilisation_ia,
    q1.usage_ia_perso_avant_expe,
    q1.usage_ia_pro_avant_expe,
    q1.craintes_usage_ia_pro,
    q3.temps_fonction_exercee,
    q3.genre,
    q3.frequence_utilisation,
    q3.evolution_usage,
    e.import_timestamp,
    e.snapshot_id,
    e.snapshot_id_parent
from {{ source('assistant_ia', 'experimentateurs') }} as e
left join {{ source('assistant_ia', 'q1') }} as q1
    on e.no_id = q1.no_id
left join {{ source('assistant_ia', 'q3') }} as q3
    on e.no_id = q3.no_id
left join {{ source('assistant_ia', 'ref_direction') }} as direction
    on q1.id_direction = direction.id
left join {{ source('assistant_ia', 'ref_domaine_professionnel') }} as domaine_professionnel
    on q1.id_domaine_professionnel = domaine_professionnel.id
left join {{ source('assistant_ia', 'ref_niveau_appropriation') }} as niveau_appropriation
    on q1.id_niveau_d_utilisation_ia = niveau_appropriation.id
order by e.no_id, e.id_row, q1.id_row, q3.id_row, direction.id_row, domaine_professionnel.id_row, niveau_appropriation.id_row
