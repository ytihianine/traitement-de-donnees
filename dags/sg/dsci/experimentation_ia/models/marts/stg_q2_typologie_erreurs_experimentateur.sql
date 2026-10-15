
-- Dataset : q2_typologie_erreurs_experimentateur
select
  -- q2_typologie_erreurs
  te.id_row,
  te.no_id,
  te.id_types_d_erreurs_frequentes2,
  te.import_timestamp,
  te.snapshot_id,
  te.snapshot_id_parent,
  -- ref_type_erreur_ia
  rteia.type_erreur,
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
from {{ source('assistant_ia', 'q2_typologie_erreurs') }} te
left join {{ ref('stg_dim_experimentateurs') }} expe
  on expe.no_id = te.no_id
    and expe.snapshot_id = te.snapshot_id
left join {{ source('assistant_ia', 'ref_type_erreur_ia') }} rteia
  on rteia.id = te.id_types_d_erreurs_frequentes2
    and rteia.snapshot_id = te.snapshot_id
order by
  te.no_id,
  te.id_row
