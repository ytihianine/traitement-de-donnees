
select
    q1.id_row,
    q1.id,
    q1.no_id,
    q1.tranche_age,
    q1.categorie_emploi,
    q1.statut,
    q1.metier,
    q1.situation_d_encadrement,
    q1.autres_experimentateurs,
    q1.usage_ia_perso_avant_expe,
    q1.usage_ia_pro_avant_expe,
    q1.craintes_usage_ia_pro,
    q1.raisons_des_craintes,
    q1.attentes_experimentation,
    q1.autres_cas_usage_transverse,
    q1.cas_d_usage_metier,
    q1.formation_suivie_usage_ia_,
    q1.autre_formation_suivie,
    q1.autre_besoin_accompagnement,
    q1.besoin_acculturation_encadrement,
    q1.id_direction,
    direction.direction,
    direction.direction_regroupee,
    q1.id_domaine_professionnel,
    domaine_professionnel.domaine as domaine_professionnel,
    q1.id_niveau_d_utilisation_ia,
    niveau_appropriation.niveau_appropriation_libelle_court as niveau_d_utilisation_ia,
    q1.import_timestamp,
    q1.snapshot_id,
    q1.snapshot_id_parent
from {{ source('assistant_ia', 'q1') }} as q1
left join {{ source('assistant_ia', 'ref_direction') }} as direction
    on q1.id_direction = direction.id
left join {{ source('assistant_ia', 'ref_domaine_professionnel') }} as domaine_professionnel
    on q1.id_domaine_professionnel = domaine_professionnel.id
left join {{ source('assistant_ia', 'ref_niveau_appropriation') }} as niveau_appropriation
    on q1.id_niveau_d_utilisation_ia = niveau_appropriation.id
order by q1.no_id, q1.id_row, direction.id_row, domaine_professionnel.id_row, niveau_appropriation.id_row
