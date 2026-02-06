DROP SCHEMA IF EXISTS cartographie_remuneration CASCADE;
CREATE SCHEMA cartographie_remuneration;

/*
    Référentiels
*/
CREATE TABLE cartographie_remuneration."ref_categorie_ecole" (
  "id" integer PRIMARY KEY,
  "categorie_d_ecole" text
);

CREATE TABLE cartographie_remuneration."ref_niveau_diplome" (
  "id" integer PRIMARY KEY,
  "annee_bac" text,
  "niveau_diplome" text
);

CREATE TABLE cartographie_remuneration."ref_libelle_diplome" (
  "id" integer PRIMARY KEY,
  "id_categorie_ecole" int,
  "libelle_diplome" text,
  "id_niveau_diplome_associe" int,
  "instruction" text,
  FOREIGN KEY ("id_categorie_ecole") REFERENCES cartographie_remuneration."ref_categorie_ecole" ("id"),
  FOREIGN KEY ("id_niveau_diplome_associe") REFERENCES cartographie_remuneration."ref_niveau_diplome" ("id")
);

CREATE TABLE cartographie_remuneration."ref_position" (
  "id" integer PRIMARY KEY,
  "id_niveau_diplome" int,
  "position" int,
  "im_minimal" int,
  "im_maximal" int,
  FOREIGN KEY ("id_niveau_diplome") REFERENCES cartographie_remuneration."ref_niveau_diplome" ("id")
);

CREATE TABLE cartographie_remuneration."ref_valeur_point_indice" (
  "id" integer PRIMARY KEY,
  "valeur_point_d_indice" FLOAT,
  "date_d_application" date
);

CREATE TABLE cartographie_remuneration."ref_base_remuneration" (
  "id" integer PRIMARY KEY,
  "base_remuneration" text
);

CREATE TABLE cartographie_remuneration."ref_base_revalorisation" (
  "id" integer PRIMARY KEY,
  "base_revalorisation" text
);

CREATE TABLE cartographie_remuneration."ref_fonction_dge" (
  "id" integer PRIMARY KEY,
  "fonction_dge" text,
  "fonction_dge_libelle_long" text
);


/*
    Données
*/
CREATE TABLE cartographie_remuneration."agent" (
  "matricule_agent" BIGINT UNIQUE,
  "nom_usuel" TEXT,
  "prenom" TEXT,
  "genre" TEXT,
  -- "date_de_naissance" DATE,
  "age" INTEGER,
  PRIMARY KEY ("matricule_agent")
);

CREATE TABLE cartographie_remuneration."agent_carriere" (
  "matricule_agent" BIGINT,
  "categorie" TEXT,
  "qualite_statutaire" TEXT,
  -- "date_acces_corps" DATE,
  "corps" TEXT,
  "grade" TEXT,
  "echelon" INTEGER,
  "indice_majore" INTEGER,
  "dge_perimetre" TEXT,
  PRIMARY KEY ("matricule_agent"),
  FOREIGN KEY ("matricule_agent") REFERENCES cartographie_remuneration."agent" ("matricule_agent")
);


DROP TABLE cartographie_remuneration."agent_remuneration";
CREATE TABLE cartographie_remuneration."agent_remuneration" (
  "matricule_agent" BIGINT,
  "mois_analyse" TEXT,
  "libelle_element" TEXT,
  "montant_a_deduire" DOUBLE PRECISION,
  "montant_a_payer" DOUBLE PRECISION,
  "uuid" uuid,
  PRIMARY KEY ("matricule_agent", "uuid")
  -- FOREIGN KEY ("matricule_agent") REFERENCES cartographie_remuneration."agent" ("matricule_agent")
);


DROP TABLE cartographie_remuneration."agent_remuneration_complement" CASCADE;
CREATE TABLE cartographie_remuneration."agent_remuneration_complement" (
  "id" integer,
  "matricule_agent" BIGINT,
  "id_base_remuneration" INT,
  "plafond_part_variable" FLOAT,
  "plafond_part_variable_collective" FLOAT,
  "present_cartographie" BOOLEAN,
  "observations" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("matricule_agent"),
  -- FOREIGN KEY ("matricule_agent") REFERENCES cartographie_remuneration."agent" ("matricule_agent"),
  FOREIGN KEY ("id_base_remuneration") REFERENCES cartographie_remuneration."ref_base_remuneration" ("id")
);


DROP TABLE cartographie_remuneration."agent_contrat";
CREATE TABLE cartographie_remuneration."agent_contrat" (
  "matricule_agent" int,
  "mois_analyse" text,
  "date_debut_contrat_actuel" date,
  "date_fin_contrat_previsionnelle_actuel" date,
  "date_cdisation" date,
  PRIMARY KEY ("matricule_agent")
);


DROP TABLE cartographie_remuneration."agent_contrat_complement";
CREATE TABLE cartographie_remuneration."agent_contrat_complement" (
  "id" int,
  "matricule_agent" int,
  "date_premier_contrat_mef" date,
  "date_entree_dge" date,
  "duree_contrat_en_cours_dge" integer,
  "duree_contrat_en_cours_auto_dge" integer,
  "duree_cumulee_contrats_tout_contrat_mef" text,
  "date_de_cdisation" date,
  "duree_en_jours_si_coupure_de_contrat" integer,
  "id_fonction_dge" integer,
  PRIMARY KEY ("id"),
  UNIQUE ("matricule_agent"),
  FOREIGN KEY ("id_fonction_dge") REFERENCES cartographie_remuneration."ref_fonction_dge" ("id")
  -- FOREIGN KEY ("matricule_agent") REFERENCES cartographie_remuneration."agent" ("matricule_agent")
);


CREATE TABLE cartographie_remuneration."agent_diplome" (
  "id" int,
  "matricule_agent" int,
  "annee_d_obtention" numeric,
  "id_libelle_diplome" int,
  "id_categorie_d_ecole" int,
  "id_niveau_diplome_associe" int,
  PRIMARY KEY ("id"),
  -- FOREIGN KEY ("matricule_agent") REFERENCES cartographie_remuneration."agent" ("matricule_agent"),
  FOREIGN KEY ("id_libelle_diplome") REFERENCES cartographie_remuneration."ref_libelle_diplome" ("id"),
  FOREIGN KEY ("id_categorie_d_ecole") REFERENCES cartographie_remuneration."ref_categorie_ecole" ("id"),
  FOREIGN KEY ("id_niveau_diplome_associe") REFERENCES cartographie_remuneration."ref_niveau_diplome" ("id")
);




CREATE TABLE cartographie_remuneration."agent_revalorisation" (
  "id" int,
  "matricule_agent" int,
  "id_base_revalorisation" int,
  "date_derniere_revalorisation" date,
  "valorisation_validee" float,
  "historique" text,
  "date_dernier_renouvellement" date,
  PRIMARY KEY ("id"),
  -- FOREIGN KEY ("matricule_agent") REFERENCES cartographie_remuneration."agent" ("matricule_agent"),
  FOREIGN KEY ("id_base_revalorisation") REFERENCES cartographie_remuneration."ref_base_revalorisation" ("id")
);


CREATE TABLE cartographie_remuneration."agent_experience_pro" (
  "matricule_agent" int,
  "exp_pro_totale_annee" integer,
  "exp_qualifiante_sur_le_poste_annee" integer,
  "exp_pro_totale_mois" double precision,
  "exp_qualifiante_sur_le_poste_mois" double precision,
  "id_position_grille" int,
  "experience_pro_qualifiante_sur_poste" double precision,
  "experience_pro_totale" double precision,
  PRIMARY KEY ("matricule_agent"),
  -- FOREIGN KEY ("matricule_agent") REFERENCES cartographie_remuneration."agent" ("matricule_agent"),
  FOREIGN KEY ("id_position_grille") REFERENCES cartographie_remuneration."ref_position" ("id")
);


/*
	VIEWS
*/
CREATE MATERIALIZED VIEW cartographie_remuneration.vw_agent_cartographie_remuneration AS
WITH cte_niveau_diplome AS (
  SELECT
    crad.matricule_agent as matricule_agent,
    crad.id_niveau_diplome as id_niveau_diplome,
    crrnd.niveau_diplome as niveau_diplome,
    crad.libelle_diplome as libelle_diplome,
    crad.annee_d_obtention as annee_d_obtention
  FROM cartographie_remuneration.ref_niveau_diplome crrnd
  LEFT JOIN cartographie_remuneration.agent_diplome crad
    ON crrnd.id = crad.id_niveau_diplome
)
SELECT
  -- agent
  cra.matricule_agent,
  cra.nom_usuel,
  cra.prenom,
  cra.date_de_naissance,
  date_part('year', cra.date_de_naissance)::int as annee_naissance,
  cra.genre,
  cra.qualite_statutaire,
  cra.date_acces_corps,
  date_part('year', cra.date_acces_corps)::int as annee_acces_corps,
  cra.corps,
  cra.grade,
  cra.echelon,
  -- agent_contrat
  crac.date_debut_contrat_actuel_dge,
  crac.date_entree_dge,
  -- agent_experience_pro
  craep.experience_pro_totale,
  craep.experience_pro_qualifiante_sur_poste,
  -- agent_poste
  crap.categorie,
  crap.libelle_du_poste,
  crap.fonction_dge_libelle_court,
  crap.fonction_dge_libelle_long,
  crap.date_recrutement_structure,
  -- agent_remuneration
  crar.indice_majore,
  crar.type_indemnitaire,
  crar.region_indemnitaire,
  crar.region_indemnitaire_valeur,
  crar.total_indemnitaire_annuel,
  crar.total_annuel_ifse,
  crar.totale_brute_annuel,
  crar.plafond_part_variable,
  crar.plafond_part_variable_collective,
  CASE
    WHEN crar.plafond_part_variable IS NULL AND crar.plafond_part_variable_collective IS NULL
      THEN NULL
    ELSE
      COALESCE(crar.plafond_part_variable, 0) + COALESCE(crar.plafond_part_variable_collective, 0)
  END as plafond_part_variable_totale,
  crar.present_cartographie,
  -- cte_diplome
  cte_diplome.niveau_diplome,
  cte_diplome.id_niveau_diplome,
  cte_diplome.libelle_diplome,
  cte_diplome.annee_d_obtention
FROM cartographie_remuneration.agent cra
LEFT JOIN cartographie_remuneration.agent_contrat crac
  ON cra.matricule_agent = crac.matricule_agent
LEFT JOIN cartographie_remuneration.agent_experience_pro craep
  ON cra.matricule_agent = craep.matricule_agent
LEFT JOIN cartographie_remuneration.agent_poste crap
  ON cra.matricule_agent = crap.matricule_agent
LEFT JOIN cartographie_remuneration.agent_remuneration crar
  ON cra.matricule_agent = crar.matricule_agent
LEFT JOIN cte_niveau_diplome cte_diplome
  ON cra.matricule_agent = cte_diplome.matricule_agent
;
