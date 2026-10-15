
-- Dataset : q1_experimentateurs
select
  -- Questionnaire 1
  q1.id_row,
  q1.no_id,
  q1.metier,
  q1.raisons_des_craintes,
  q1.attentes_experimentation,
  q1.autres_cas_usage_transverse,
  q1.cas_d_usage_metier,
  q1.formation_suivie_usage_ia_,
  q1.autre_formation_suivie,
  q1.autre_besoin_accompagnement,
  q1.besoin_acculturation_encadrement,
  q1.import_timestamp,
  q1.snapshot_id,
  q1.snapshot_id_parent,
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
from {{ ref('stg_fct_q1') }} q1
left join {{ ref('stg_dim_experimentateurs')}} expe
  on q1.no_id = expe.no_id
    and q1.import_timestamp = expe.import_timestamp
order by
    q1.no_id, expe.id_direction,
