
-- Dataset : q2_facteurs_progression_experimentateur
select
  -- q2_facteurs_progression
  fp.id_row,
  fp.id,
  fp.no_id,
  fp.id_facteurs_de_progression,
  fp.import_timestamp,
  fp.snapshot_id,
  fp.snapshot_id_parent,
  -- ref_facteur_progression
  rfp.facteurs,
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
from {{ source('assistant_ia', 'q2_facteurs_progression') }} fp
left join {{ ref('stg_dim_experimentateurs') }} expe
  on expe.no_id = fp.no_id
    and expe.snapshot_id = fp.snapshot_id
left join {{ source('assistant_ia', 'ref_facteur_progression') }} rfp
  on rfp.id = fp.id_facteurs_de_progression
    and rfp.snapshot_id = fp.snapshot_id
order by
  fp.no_id,
  fp.id_row
