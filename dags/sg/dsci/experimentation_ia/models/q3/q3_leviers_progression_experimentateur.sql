
-- Dataset : q3_leviers_progression_experimentateur
select
  -- q3_leviers_progression
  q3_lp.id_row,
  q3_lp.id,
  q3_lp.no_id,
  q3_lp.id_leviers_progression,
  q3_lp.import_timestamp,
  q3_lp.snapshot_id,
  q3_lp.snapshot_id_parent,
  -- ref_levier_progression
  rlp.leviers,
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
from {{ source('assistant_ia', 'q3_leviers_progression') }} q3_lp
left join {{ ref('stg_dim_experimentateurs') }} expe
  on expe.no_id = q3_lp.no_id
    and expe.snapshot_id = q3_lp.snapshot_id
left join {{ source('assistant_ia', 'ref_levier_progression') }} rlp
  on rlp.id = q3_lp.id_leviers_progression
    and rlp.snapshot_id = q3_lp.snapshot_id
order by
  q3_lp.no_id,
  q3_lp.id_row
