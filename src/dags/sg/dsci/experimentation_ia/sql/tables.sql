DROP SCHEMA IF EXISTS assistant_ia CASCADE;
CREATE SCHEMA IF NOT EXISTS assistant_ia;

/*
    Référentiels questionnaire 1
*/

CREATE TABLE assistant_ia."ref_q1_direction" (
  "id" INTEGER PRIMARY KEY,
  "direction" TEXT
);

CREATE TABLE assistant_ia."ref_q5_domaine" (
  "id" INTEGER PRIMARY KEY,
  "domaine" TEXT
);
CREATE TABLE assistant_ia."ref_q6_niveau_utilisation" (
  "id" INTEGER PRIMARY KEY,
  "niveau_d_appropriation" TEXT
);
CREATE TABLE assistant_ia."ref_q9_cas_usage" (
  "id" INTEGER PRIMARY KEY,
  "cas_d_usage" TEXT
);

/*
   Référentiels questionnaire 2
*/

CREATE TABLE assistant_ia."ref_q28_raisons_perte" (
  "id" INTEGER PRIMARY KEY,
  "raisons" TEXT
);
CREATE TABLE assistant_ia."ref_q25_impact_observe" (
  "id" INTEGER PRIMARY KEY,
  "observation" TEXT
);
CREATE TABLE assistant_ia."ref_q24_impact_identifie" (
  "id" INTEGER PRIMARY KEY,
  "impacts" TEXT
);
CREATE TABLE assistant_ia."ref_q23_taux_correction" (
  "id" INTEGER PRIMARY KEY,
  "taux_de_correction" TEXT
);
CREATE TABLE assistant_ia."ref_q22_typologie_erreurs" (
  "id" INTEGER PRIMARY KEY,
  "erreurs" TEXT
);
CREATE TABLE assistant_ia."ref_q20_autres_ia" (
  "id" INTEGER PRIMARY KEY,
  "comparaisons" TEXT
);
CREATE TABLE assistant_ia."ref_q16_taches" (
  "id" INTEGER PRIMARY KEY,
  "taches" TEXT
);
CREATE TABLE assistant_ia."ref_q14_evolution_craintes" (
  "id" INTEGER PRIMARY KEY,
  "evolutions" TEXT
);
CREATE TABLE assistant_ia."ref_q13_facteurs_progression"(
  "id" INTEGER PRIMARY KEY,
  "facteurs" TEXT
);
CREATE TABLE assistant_ia."ref_q10_principaux_freins" (
  "id" INTEGER PRIMARY KEY,
  "freins" TEXT
);
CREATE TABLE assistant_ia."ref_q6_participation_programme" (
  "id" INTEGER PRIMARY KEY,
  "participation" TEXT
);
CREATE TABLE assistant_ia."ref_q5_formation_suivie" (
  "id" INTEGER PRIMARY KEY,
  "formation_suivie" TEXT
);
CREATE TABLE assistant_ia."ref_q3_niveau_2" (
  "id" INTEGER PRIMARY KEY,
  "niveau" TEXT
);
CREATE TABLE assistant_ia."ref_q7_accords" (
  "id" INTEGER PRIMARY KEY,
  "reponses" TEXT
);

/*
    Repartition par entité
*/

CREATE TABLE assistant_ia."quota_par_entite" (
  "id" integer PRIMARY KEY,
  "experimentation_demarree" boolean,
  "entite" text,
  "nbre_d_acces_previsionnels" int,
  "nb_acces_demande" int,
  "code" text,
  "nbre_connexion_effective" int,
  "nb_de_reponses_au_questionnaire" int,
  "nb_reponse_q2" int,
  "relance_dsci" text,
  "appel_a_candidature_dsci" text,
  "referent_ia" text,
  "courriel" text
);


/*
    Experimentateurs
*/
CREATE TABLE assistant_ia."experimentateurs"(
    "id" integer PRIMARY KEY,
    "no_id" text,
    "entite" text,
    --"courriel" text,
    "courriel_corrige" text,
    "connecte_" text,
    "reponse_au_questionnaire_1" text,
    "reponse_au_questionnaire_2" text,
    "nom_Prenom" text,
    "parti" text,
    "service" text,
    "metier" text,
    "cas_d_usages" text
);


/*
    Questionnaire 1 : Profil des expérimentateurs
*/
DROP TABLE IF EXISTS assistant_ia."questionnaire_1" CASCADE;
CREATE TABLE assistant_ia."questionnaire_1"(
    "id" INTEGER PRIMARY KEY,
    "mail_corrige" TEXT,
    "id_direction" INTEGER,
    "tranche_age" TEXT,
    "categorie_emploi" TEXT,
    "statut" TEXT,
    "id_domaine_professionnel" INTEGER,
    "metier" TEXT,
    "situation_d_encadrement" TEXT,
    "autres_experimentateurs" TEXT,
    "id_niveau_d_utilisation_ia" INTEGER,
    "usage_ia_perso_avant_expe" TEXT,
    "usage_ia_pro_avant_expe" TEXT,
    "craintes_usage_ia_pro" TEXT,
    "raisons_des_craintes" TEXT,
    "attentes_experimentation" TEXT,
    --"id_cas_d_usage_envisages" TEXT,
    "autres_cas_usage_transverse" TEXT,
    "cas_d_usage_metier" TEXT,
    "formation_suivie_usage_ia_" TEXT,
    "autre_formation_suivie" TEXT,
    --"besoin_accompagnement" TEXT,
    "autre_besoin_accompagnement" TEXT,
    "besoin_acculturation_encadrement" TEXT
);
ALTER TABLE "questionnaire_1" ADD FOREIGN KEY ("id_direction") REFERENCES assistant_ia."ref_q1_direction" ("id");
ALTER TABLE "questionnaire_1" ADD FOREIGN KEY ("id_domaine_professionnel") REFERENCES assistant_ia."ref_q5_domaine" ("id");
ALTER TABLE "questionnaire_1" ADD FOREIGN KEY ("id_niveau_d_utilisation_ia") REFERENCES assistant_ia."ref_q6_niveau_utilisation"("id");

----  Table de liaison  cas d'usage envisagés--------------
DROP TABLE IF EXISTS assistant_ia."questionnaire_1_cas_usage" CASCADE;
CREATE TABLE assistant_ia."questionnaire_1_cas_usage"(
    "id" bigint PRIMARY KEY,
    "id_questionnaire_1" INTEGER,
    "id_cas_d_usage_envisages" INTEGER,
  UNIQUE ("id_questionnaire_1", "id_cas_d_usage_envisages"),
  FOREIGN KEY ("id_questionnaire_1") REFERENCES assistant_ia."questionnaire_1"("id"),
  FOREIGN KEY ("id_cas_d_usage_envisages") REFERENCES assistant_ia."ref_q9_cas_usage"("id")
);

CREATE TABLE assistant_ia."questionnaire_1_besoins_accompagnement" (
	"id" bigint PRIMARY KEY,
	"id_questionnaire_1" integer,
	"besoin_accompagnement" text,
	UNIQUE ("id_questionnaire_1", "besoin_accompagnement"),
	FOREIGN KEY ("id_questionnaire_1") REFERENCES assistant_ia."questionnaire_1"("id")
);


/*
    Questionnaire 2 : Retour des expérimentateurs
*/

DROP TABLE IF EXISTS assistant_ia."questionnaire_2" CASCADE;
CREATE TABLE assistant_ia."questionnaire_2" (
    "id" INTEGER PRIMARY KEY,
    "mail_corrige" TEXT,
    "direction" TEXT,
    --"types_d_interactions_mef" TEXT,
    "autres_types_d_interactions" TEXT,
    "id_niveau_d_usage_ia_post_expe_" INTEGER,
    "frequence_d_usage_assistant_ia" TEXT,
    "autres_formation_ia" TEXT,
    "raison_non_participation_rdv" TEXT,
    "autre_besoin_accompagnement" TEXT,
    "apprentissage_assistant_ia_ressenti_" TEXT,
    "difficultes_techniques_rencontrees2" TEXT,
    "autres_difficultes" TEXT,
    "autres_freins" TEXT,
    "id_recommandation_collegues_mef" INTEGER,
    "id_sensation_montee_en_competences" INTEGER,
    "autres_sources_de_progression" TEXT,
    "id_evolution_des_craintes_initiales" INTEGER,
    "id_utilite_metier_mef" INTEGER,
    "autres_taches_realisees" TEXT,
    "decouverte_d_usages_inattendus" TEXT,
    "les_usages_inattendus" TEXT,
    "mode_de_decouverte_usages" TEXT,
    "autre_mode_de_decouverte" TEXT,
    "id_diminution_d_usage_ia_non_souveraines" INTEGER,
    "id_comparaison_autres_ia" INTEGER,
    "frequence_des_erreurs" TEXT,
    "autres_types_d_erreurs" TEXT,
    "cas_usage_principal_teste" TEXT,
    "temps_economise_par_semaine" TEXT,
    "cu1_nombre_echanges_moyens_affinage_reponse" TEXT,
    "id_taux_moyen_de_correction_rep_assistant" INTEGER,
    "pertinence_assistant_ia" TEXT,
    "commentaires" TEXT,
    "deuxieme_cas_d_usage_teste" TEXT,
    "cu2_temps_economise_par_semaine" TEXT,
    "cu2_nombre_echanges_moyens" TEXT,
    "id_cu2_taux_moyen_de_correction_rep_assistant" INTEGER,
    "cu2_pertinence_assistant_ia" TEXT,
    "commentaires2" TEXT,
    "troisieme_cas_d_usage" TEXT,
    "cu3_temps_economise_par_semaine" TEXT,
    "cu3_nombre_echanges_moyens_affinage_reponse" TEXT,
    "id_cu3_taux_moyen_de_correction_rep_assistant" INTEGER,
    "cu3_pertinence_assistant_ia" TEXT,
    "commentaires3" TEXT,
    "autres_impacts_identifies" TEXT,
    "autres_impacts_observes" TEXT,
    "impact_sur_le_temps_de_travail" TEXT,
    "estimation_globale_gain_de_temps" TEXT,
    "id_raisons_perte_de_temps" INTEGER,
    "autres_raisons" TEXT,
    "id_ia_favorise_relations_humaines_" INTEGER
);
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_niveau_d_usage_ia_post_expe_") REFERENCES assistant_ia."ref_q3_niveau_2"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_recommandation_collegues_mef") REFERENCES assistant_ia."ref_q7_accords"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_sensation_montee_en_competences") REFERENCES assistant_ia."ref_q7_accords"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_evolution_des_craintes_initiales") REFERENCES assistant_ia."ref_q14_evolution_craintes"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_utilite_metier_mef") REFERENCES assistant_ia."ref_q7_accords"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_diminution_d_usage_ia_non_souveraines") REFERENCES assistant_ia."ref_q7_accords"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_comparaison_autres_ia") REFERENCES assistant_ia."ref_q20_autres_ia"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_taux_moyen_de_correction_rep_assistant") REFERENCES assistant_ia."ref_q23_taux_correction"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_cu2_taux_moyen_de_correction_rep_assistant") REFERENCES assistant_ia."ref_q23_taux_correction"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_cu3_taux_moyen_de_correction_rep_assistant") REFERENCES assistant_ia."ref_q23_taux_correction"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_raisons_perte_de_temps") REFERENCES assistant_ia."ref_q28_raisons_perte"("id");
ALTER TABLE "questionnaire_2" ADD FOREIGN KEY ("id_ia_favorise_relations_humaines_") REFERENCES assistant_ia."ref_q7_accords"("id");


----- Tables de liaisons du questionnaire 2--------------------

DROP TABLE IF EXISTS assistant_ia."questionnaire2_formation_suivie" CASCADE;
CREATE TABLE assistant_ia."questionnaire2_formation_suivie" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_formation_ia_suivie_post_expe_" INTEGER,
    UNIQUE ("id_questionnaire_2", "id_formation_ia_suivie_post_expe_" ),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_formation_ia_suivie_post_expe_") REFERENCES assistant_ia."ref_q5_formation_suivie"("id")
);

CREATE TABLE assistant_ia."questionnaire2_typologie_interaction" (
	"id" bigint PRIMARY KEY,
	"id_questionnaire_2" integer,
	"types_d_interactions_mef" text,
	UNIQUE ("id_questionnaire_2", "types_d_interactions_mef"),
	FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id")
);

CREATE TABLE assistant_ia."questionnaire2_participation" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_participation_programme_rdv" INTEGER,
    UNIQUE ("id_questionnaire_2","id_participation_programme_rdv"),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_participation_programme_rdv") REFERENCES assistant_ia."ref_q6_participation_programme"("id")
);

CREATE TABLE assistant_ia."questionnaire2_freins" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_freins_a_l_utilisation" INTEGER,
    UNIQUE ("id_questionnaire_2","id_freins_a_l_utilisation"),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_freins_a_l_utilisation") REFERENCES assistant_ia."ref_q10_principaux_freins"("id")
);

CREATE TABLE assistant_ia."questionnaire2_facteurs_progression" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_facteurs_de_progression" INTEGER,
    UNIQUE ("id_questionnaire_2","id_facteurs_de_progression"),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_facteurs_de_progression") REFERENCES assistant_ia."ref_q13_facteurs_progression"("id")
);

CREATE TABLE assistant_ia."questionnaire2_taches" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_taches_realisees_avec_ia" INTEGER,
    UNIQUE ("id_questionnaire_2","id_taches_realisees_avec_ia"),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_taches_realisees_avec_ia") REFERENCES assistant_ia."ref_q16_taches"("id")
);

CREATE TABLE assistant_ia."questionnaire2_typologie_erreurs" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_types_d_erreurs_frequentes2" INTEGER,
    UNIQUE ("id_questionnaire_2","id_types_d_erreurs_frequentes2"),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_types_d_erreurs_frequentes2") REFERENCES assistant_ia."ref_q22_typologie_erreurs"("id")
);

CREATE TABLE assistant_ia."questionnaire2_observation_impact" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_observations_des_impacts" INTEGER,
    UNIQUE ("id_questionnaire_2","id_observations_des_impacts"),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_observations_des_impacts") REFERENCES assistant_ia."ref_q25_impact_observe"("id")
);

CREATE TABLE assistant_ia."questionnaire2_impact_identifie" (
    "id" bigint PRIMARY KEY,
    "id_questionnaire_2" INTEGER,
    "id_impacts_identifies_au_travail" INTEGER,
    UNIQUE ("id_questionnaire_2","id_impacts_identifies_au_travail" ),
    FOREIGN KEY ("id_questionnaire_2") REFERENCES assistant_ia."questionnaire_2"("id"),
    FOREIGN KEY ("id_impacts_identifies_au_travail" ) REFERENCES assistant_ia."ref_q24_impact_identifie"("id")
);
