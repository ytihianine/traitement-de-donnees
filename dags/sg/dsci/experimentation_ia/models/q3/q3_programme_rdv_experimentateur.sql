
-- Dataset : q3_programme_rdv_experimentateur
select
  -- q3_programme_rdv
  q3_rdv.id_row,
  q3_rdv.no_id,
  q3_rdv.id_programme_de_rdv,
  q3_rdv.import_timestamp,
  q3_rdv.snapshot_id,
  q3_rdv.snapshot_id_parent,
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
from {{ source('assistant_ia', 'q3_programme_rdv') }} q3_rdv
left join {{ ref('dim_experimentateurs') }} expe
  on expe.no_id = q3_rdv.no_id
    and expe.snapshot_id = q3_rdv.snapshot_id
left join {{ source('assistant_ia', 'ref_participation_programme') }} rpr
  on rpr.id = q3_rdv.id_programme_de_rdv
    and rpr.snapshot_id = q3_rdv.snapshot_id
order by
  q3_rdv.no_id,
  q3_rdv.id_row
