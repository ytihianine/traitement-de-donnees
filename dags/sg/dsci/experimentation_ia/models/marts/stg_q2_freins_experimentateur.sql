
-- Dataset : q2_freins_experimentateur
select
  -- q2_freins
  frein.id_row,
  frein.no_id,
  frein.id_freins_a_l_utilisation,
  frein.import_timestamp,
  frein.snapshot_id,
  frein.snapshot_id_parent,
  -- ref_frein_utilisation
  rfu.freins,
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
from {{ source('assistant_ia', 'q2_freins') }} frein
left join {{ ref('stg_dim_experimentateurs') }} expe
  on expe.no_id = frein.no_id
    and expe.snapshot_id = frein.snapshot_id
left join {{ source('assistant_ia', 'ref_frein_utilisation') }} rfu
  on rfu.id = frein.id_freins_a_l_utilisation
    and rfu.snapshot_id = frein.snapshot_id
order by
  frein.no_id,
  frein.id_row
