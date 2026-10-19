
-- Dataset : q2_programme_rdv_experimentateur
select
  -- q2_programme_rdv
  q2_rdv.id_row,
  q2_rdv.no_id,
  q2_rdv.id_programme_de_rdv,
  q2_rdv.import_timestamp,
  q2_rdv.snapshot_id,
  q2_rdv.snapshot_id_parent,
  -- ref_participation_programme
  rpr.participation,
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
from {{ source('assistant_ia', 'q2_programme_rdv') }} q2_rdv
left join {{ ref('dim_experimentateurs') }} expe
  on expe.no_id = q2_rdv.no_id
    and expe.snapshot_id = q2_rdv.snapshot_id
left join {{ source('assistant_ia', 'ref_participation_programme') }} rpr
  on rpr.id = q2_rdv.id_programme_de_rdv
    and rpr.snapshot_id = q2_rdv.snapshot_id
order by
  q2_rdv.no_id,
  q2_rdv.id_row
