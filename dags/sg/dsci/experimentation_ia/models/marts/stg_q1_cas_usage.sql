
-- Dataset : q1_cas_usage
select
  -- q1_cas_usage
  q1_cu.id_row,
  q1_cu.id,
  q1_cu.no_id,
  q1_cu.id_cas_d_usage_envisages,
  q1_cu.import_timestamp,
  q1_cu.snapshot_id,
  q1_cu.snapshot_id_parent,
  -- ref_cas_usage
  rcu.cas_d_usage,
  rcu.cas_usage_simplifie,
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
from {{ source('assistant_ia', 'q1_cas_usage') }} q1_cu
left join {{ ref('stg_dim_experimentateurs') }} expe
  on expe.no_id = q1_cu.no_id
    and expe.snapshot_id = q1_cu.snapshot_id
left join {{ source('assistant_ia', 'ref_cas_usage') }} rcu
  on q1_cu.id_cas_d_usage_envisages = rcu.id
    and q1_cu.snapshot_id = rcu.snapshot_id
order by
  q1_cu.no_id,
  q1_cu.id_row
