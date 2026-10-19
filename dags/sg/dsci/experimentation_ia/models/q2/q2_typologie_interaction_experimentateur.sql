
-- Dataset : q2_typologie_interaction_experimentateur
select
  -- q2_typologie_interaction
  ti.id_row,
  ti.id,
  ti.no_id,
  ti.types_d_interactions_mef,
  ti.import_timestamp,
  ti.snapshot_id,
  ti.snapshot_id_parent,
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
from {{ source('assistant_ia', 'q2_typologie_interaction') }} ti
left join {{ ref('stg_dim_experimentateurs') }} expe
  on expe.no_id = ti.no_id
    and expe.snapshot_id = ti.snapshot_id
order by
  ti.no_id,
  ti.id_row
