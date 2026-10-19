
-- Dataset : evolution_question_similaire_experimentateur
select
  -- dim_experimentateurs
  dim_exp.no_id,
  dim_exp.connecte_,
  dim_exp.reponse_au_questionnaire_1,
  dim_exp.reponse_au_questionnaire_2,
  dim_exp.reponse_au_questionnaire_3,
  dim_exp.parti,
  dim_exp.direction,
  dim_exp.direction_regroupee,
  dim_exp.tranche_age,
  dim_exp.categorie_emploi,
  dim_exp.statut,
  dim_exp.domaine_professionnel,
  dim_exp.situation_d_encadrement,
  dim_exp.autres_experimentateurs,
  dim_exp.niveau_d_utilisation_ia,
  dim_exp.usage_ia_perso_avant_expe,
  dim_exp.usage_ia_pro_avant_expe,
  dim_exp.craintes_usage_ia_pro,
  dim_exp.temps_fonction_exercee,
  dim_exp.genre,
  dim_exp.frequence_utilisation,
  dim_exp.evolution_usage,
  dim_exp.import_timestamp,
  dim_exp.snapshot_id,
  dim_exp.snapshot_id_parent,
  -- Les evolutions
  CONCAT('Q1 - ', COALESCE(NULLIF(TRIM(q1.niveau_d_utilisation_ia), ''), 'Non répondu')) as niveau_appropriation_q1,
  CONCAT('Q2 - ', COALESCE(NULLIF(TRIM(q2.niveau_d_usage_ia_post_expe), ''), 'Non répondu')) as niveau_appropriation_q2,
  CONCAT('Q2 - ', COALESCE(NULLIF(TRIM(q2.frequence_d_usage_assistant_ia), ''), 'Non répondu')) as frequence_usage_q2,
  CONCAT('Q3 - ', COALESCE(NULLIF(TRIM(q3.frequence_utilisation), ''), 'Non répondu')) as frequence_usage_q3,
  CONCAT('Q2 - ', COALESCE(NULLIF(TRIM(q2.recommandation_collegues_mef), ''), 'Non répondu')) as recommandation_q2,
  CONCAT('Q3 - ', COALESCE(NULLIF(TRIM(q3.recommandations), ''), 'Non répondu')) as recommandation_q3
from {{ ref('stg_dim_experimentateurs') }} dim_exp
left join {{ ref('stg_fct_q1') }} q1
  on q1.no_id = dim_exp.no_id
    and q1.import_timestamp = dim_exp.import_timestamp
left join {{ ref('stg_fct_q2') }} q2
  on q2.no_id = dim_exp.no_id
    and q2.import_timestamp = dim_exp.import_timestamp
left join {{ ref('stg_fct_q3') }} q3
  on q3.no_id = dim_exp.no_id
    and q3.import_timestamp = dim_exp.import_timestamp
order by
  dim_exp.no_id,
  dim_exp.id_row
