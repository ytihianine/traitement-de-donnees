
-- Dataset : q3_formation_suivie_experimentateur
select
  -- q3_formation_suivie
  q3_fs.id_row,
  q3_fs.id,
  q3_fs.no_id,
  q3_fs.id_formation_suivie,
  q3_fs.import_timestamp,
  q3_fs.snapshot_id,
  q3_fs.snapshot_id_parent,
  -- ref_formation_suivie
  rfs.formation_suivie,
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
from {{ source('assistant_ia', 'q3_formation_suivie') }} q3_fs
left join {{ ref('dim_experimentateurs') }} expe
  on expe.no_id = q3_fs.no_id
    and expe.snapshot_id = q3_fs.snapshot_id
left join {{ source('assistant_ia', 'ref_formation_suivie') }} rfs
  on rfs.id = q3_fs.id_formation_suivie
    and rfs.snapshot_id = q3_fs.snapshot_id
order by
  q3_fs.no_id,
  q3_fs.id_row
