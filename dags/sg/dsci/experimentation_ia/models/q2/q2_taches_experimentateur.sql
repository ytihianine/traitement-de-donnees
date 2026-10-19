
-- Dataset : q2_taches_experimentateur
select
  -- q2_taches
  taches.id_row,
  taches.id,
  taches.no_id,
  taches.id_taches_realisees_avec_ia,
  taches.import_timestamp,
  taches.snapshot_id,
  taches.snapshot_id_parent,
  -- ref_tache_realise
  rtr.taches,
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
from {{ source('assistant_ia', 'q2_taches') }} taches
left join {{ ref('stg_dim_experimentateurs') }} expe
  on expe.no_id = taches.no_id
    and expe.snapshot_id = taches.snapshot_id
left join {{ source('assistant_ia', 'ref_tache_realise') }} rtr
  on rtr.id = taches.id_taches_realisees_avec_ia
    and rtr.snapshot_id = taches.snapshot_id
order by
  taches.no_id,
  taches.id_row
