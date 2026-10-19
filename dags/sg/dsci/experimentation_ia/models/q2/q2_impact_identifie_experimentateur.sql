
-- Dataset : q2_impact_identifie_experimentateur
select
  -- q2_impact_identifie
  ii.id_row,
  ii.id,
  ii.no_id,
  ii.id_impacts_identifies_au_travail,
  ii.import_timestamp,
  ii.snapshot_id,
  ii.snapshot_id_parent,
  -- ref_impact_identifie
  rii.impact,
  -- experimentateurs
  expe.reponse_au_questionnaire_1,
  expe.reponse_au_questionnaire_2,
  expe.reponse_au_questionnaire_3,
  expe.parti,
  expe.id_direction,
  expe.direction,
  expe.direction_regroupee,
  expe.tranche_age,
  expe.categorie_emploi,
  expe.statut,
  expe.id_domaine_professionnel,
  expe.domaine_professionnel,
  expe.situation_d_encadrement,
  expe.autres_experimentateurs,
  expe.id_niveau_d_utilisation_ia,
  expe.niveau_d_utilisation_ia,
  expe.usage_ia_perso_avant_expe,
  expe.usage_ia_pro_avant_expe,
  expe.craintes_usage_ia_pro,
  expe.temps_fonction_exercee,
  expe.genre,
  expe.frequence_utilisation,
  expe.evolution_usage
from {{ source('assistant_ia', 'q2_impact_identifie') }} ii
left join {{ ref('dim_experimentateurs') }} expe
  on expe.no_id = ii.no_id
    and expe.snapshot_id = ii.snapshot_id
left join {{ source('assistant_ia', 'ref_impact_identifie') }} rii
  on rii.id = ii.id_impacts_identifies_au_travail
    and rii.snapshot_id = ii.snapshot_id
order by
  ii.no_id,
  ii.id_row
