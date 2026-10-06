DROP SCHEMA IF EXISTS assistant_ia CASCADE;
CREATE SCHEMA IF NOT EXISTS assistant_ia;


/*
    Référentiels commun
*/
CREATE TABLE assistant_ia."ref_niveau_appropriation" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "niveau_d_appropriation" TEXT,
  "niveau_appropriation_libelle_court" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_niveau_accord" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "niveau_accord" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_formation_suivie" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "formation_suivie" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_participation_programme" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "participation" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
    Référentiels questionnaire 1
*/

CREATE TABLE assistant_ia."ref_direction" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "direction" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_domaine_professionnel" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "domaine" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_cas_usage" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "cas_d_usage" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
   Référentiels questionnaire 2 & 2 bis
*/

CREATE TABLE assistant_ia."ref_raison_perte_temps" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "raisons" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_impact_observation" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "observation" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_impact_identifie" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "impact" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_taux_correction" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "taux_de_correction" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_type_erreur_ia" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "type_erreur" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_tache_realise" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "taches" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_comparaison_autres_ia" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "comparaisons" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_evolution_crainte" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "evolutions" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_facteur_progression"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "facteurs" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_frein_utilisation" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "freins" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE assistant_ia."ref_raison_non_utilisation" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "raisons" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


/*
   Référentiels questionnaire 3
*/
CREATE TABLE assistant_ia."ref_raison_non_participation"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "raisons" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_levier_progression"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "leviers" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_impact_tache_pro"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "impacts" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_impact_tache_rebarbative"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "taches_rebarbatives" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_comparaison_autre_ia"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "comparaisons" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_autre_fonctionnalite"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "fonctionnalites" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_risque"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "risques" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."ref_besoin"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" INTEGER,
  "besoins" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
    Repartition par entité
*/

CREATE TABLE assistant_ia."quota_par_entite" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" bigserial,
  "experimentation_demarree" boolean,
  "entite" text,
  "nbre_d_acces_previsionnels" NUMERIC,
  "nb_acces_demande" NUMERIC,
  "code" text,
  "nbre_connexion_effective" NUMERIC,
  "nb_de_reponses_au_questionnaire" NUMERIC,
  "nb_reponse_q2" NUMERIC,
  "nb_reponse_q3"NUMERIC,
  "relance_dsci" text,
  "appel_a_candidature_dsci" text,
  "referent_ia" text,
  "courriel" text,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


/*
    Experimentateurs
*/
CREATE TABLE assistant_ia."experimentateurs"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" text,
  "entite" text,
  "courriel" text,
  "courriel_corrige" text,
  "connecte_" text,
  "reponse_au_questionnaire_1" text,
  "reponse_au_questionnaire_2" text,
  "reponse_au_questionnaire_3" text,
  "parti" text,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


/*
    Questionnaire 1 : Profil des expérimentateurs
*/
CREATE TABLE assistant_ia."q1"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" TEXT,
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
  "autres_cas_usage_transverse" TEXT,
  "cas_d_usage_metier" TEXT,
  "formation_suivie_usage_ia_" TEXT,
  "autre_formation_suivie" TEXT,
  "autre_besoin_accompagnement" TEXT,
  "besoin_acculturation_encadrement" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

----  Table de liaison  cas d'usage envisagés--------------
CREATE TABLE assistant_ia."q1_cas_usage"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" bigserial,
  "no_id" text,
  "id_cas_d_usage_envisages" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("import_timestamp", "no_id", "id_cas_d_usage_envisages")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q1_besoins_accompagnement" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" bigserial,
  "no_id" text,
  "besoin_accompagnement" text,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("import_timestamp", "no_id", "besoin_accompagnement")
) PARTITION BY RANGE (import_timestamp);


/*
    Questionnaire 2 : Retour des expérimentateurs
*/

CREATE TABLE assistant_ia."q2" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" text,
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
  "id_ia_favorise_relations_humaines_" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id")
) PARTITION BY RANGE (import_timestamp);


----- Tables de liaisons du questionnaire 2--------------------

CREATE TABLE assistant_ia."q2_formation_suivie" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_formation_ia_suivie_post_expe_" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_formation_ia_suivie_post_expe_" )
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_typologie_interaction" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" bigserial,
  "no_id" text,
  "types_d_interactions_mef" text,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("import_timestamp", "no_id", "types_d_interactions_mef")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_participation" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_participation_programme_rdv" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id","id_participation_programme_rdv")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_freins" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_freins_a_l_utilisation" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id","id_freins_a_l_utilisation")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_facteurs_progression" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_facteurs_de_progression" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id","id_facteurs_de_progression")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_taches" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_taches_realisees_avec_ia" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id","id_taches_realisees_avec_ia")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_typologie_erreurs" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_types_d_erreurs_frequentes2" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id","id_types_d_erreurs_frequentes2")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_impact_observe" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_observations_des_impacts" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id","id_observations_des_impacts")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q2_impact_identifie" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_impacts_identifies_au_travail" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_impacts_identifies_au_travail" )
) PARTITION BY RANGE (import_timestamp);


/*
    Questionnaire 2_bis : Les agents jamais connectés
*/

CREATE TABLE assistant_ia."q2bis" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "courriel" TEXT,
    "avez_vous_deja_utilise_l_assistant_ia_" TEXT,
    "autres_raisons" TEXT,
    "ajouter_quelque_chose" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);
-- table de liasion

CREATE TABLE assistant_ia."q2bis_raisons_non_utilisation" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "courriel" TEXT,
    "id_raisons_non_utilisation" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "courriel", "id_raisons_non_utilisation" )
) PARTITION BY RANGE (import_timestamp);

/*
   Questionnaire 3 : usages et ressentis
*/

CREATE TABLE assistant_ia."q3" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" text,
  "temps_fonction_exercee" TEXT,
  "genre" TEXT,
  "frequence_utilisation" TEXT,
  "evolution_usage" TEXT,
  "quelles_raisons_facons" TEXT,
  -- Références simples (Clés étrangères)
  "id_raisons_non_participation" INTEGER,
  "id_impacts_taches_pro" INTEGER,
  "id_impacts_taches_rebarbatives" INTEGER,
  "id_comparaison_autres_ia" INTEGER,
  "id_utilisation_moindre" INTEGER,
  "id_recommandations" INTEGER,
  -- Réponses textuelles libres et commentaires
  "autres_outils" TEXT,
  "satisfaction_autre_outil" TEXT,
  "autres" TEXT,
  "evaluation_niveau_acculturation" TEXT,
  "evolution_sentiment" TEXT,
  "autres_leviers" TEXT,
  "temps_gagnes" TEXT,
  "impact_perception" TEXT,
  "sentiment_de_fierte" TEXT,
  "experimentation_interne" TEXT,
  "autres_fonctionnalites" TEXT,
  "autres_risques" TEXT,
  "etre_ambassadeur" TEXT,
  "bonnes_pratiques" TEXT,
  "autres_besoins_importants" TEXT,
  "autres_besoins_moindres" TEXT,
  "ameliorations" TEXT,
  "aspects_a_ameliorer" TEXT,
  "interface" TEXT,
  "contenu" TEXT,
  "connexions" TEXT,
  "autre_retour_libre" TEXT,
  "retours_libres" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

----- Tables de liaisons questionnaire_3

CREATE TABLE assistant_ia."q3_formation_suivie" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_formation_suivie" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_formation_suivie" )
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q3_programme_rdv" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_programme_de_rdv" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_programme_de_rdv")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q3_leviers_progression" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_leviers_progression" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_leviers_progression")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q3_fonctionnalites" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_fonctionnalites" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_fonctionnalites" )
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q3_risques_identifies" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_risques_identifies" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_risques_identifies" )
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q3_besoins_prioritaires" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_besoins_prioritaires" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_besoins_prioritaires" )
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."q3_besoins_moindres" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" bigserial,
    "no_id" text,
    "id_besoins_moindres" INTEGER,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id", "id_besoins_moindres" )
) PARTITION BY RANGE (import_timestamp);


/*
  Table des dimensions
*/

CREATE TABLE assistant_ia."dim_experimentateurs"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" text,
  "entite" text,
  "courriel" text,
  "courriel_corrige" text,
  "connecte_" text,
  "reponse_au_questionnaire_1" text,
  "reponse_au_questionnaire_2" text,
  "reponse_au_questionnaire_3" text,
  "parti" text,
  "id_direction" INTEGER,
  "direction" TEXT,
  "tranche_age" TEXT,
  "categorie_emploi" TEXT,
  "statut" TEXT,
  "id_domaine_professionnel" INTEGER,
  "domaine_professionnel" TEXT,
  "situation_d_encadrement" TEXT,
  "autres_experimentateurs" TEXT,
  "id_niveau_d_utilisation_ia" INTEGER,
  "niveau_d_utilisation_ia" TEXT,
  "usage_ia_perso_avant_expe" TEXT,
  "usage_ia_pro_avant_expe" TEXT,
  "craintes_usage_ia_pro" TEXT,
  /* Questionnaire 3 */
  "temps_fonction_exercee" TEXT,
  "genre" TEXT,
  "frequence_utilisation" TEXT,
  "evolution_usage" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."dim_q1"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" TEXT,
  "id_direction" INTEGER,
  "direction" TEXT,
  "tranche_age" TEXT,
  "categorie_emploi" TEXT,
  "statut" TEXT,
  "id_domaine_professionnel" INTEGER,
  "domaine_professionnel" TEXT,
  "metier" TEXT,
  "situation_d_encadrement" TEXT,
  "autres_experimentateurs" TEXT,
  "id_niveau_d_utilisation_ia" INTEGER,
  "niveau_d_utilisation_ia" TEXT,
  "usage_ia_perso_avant_expe" TEXT,
  "usage_ia_pro_avant_expe" TEXT,
  "craintes_usage_ia_pro" TEXT,
  "raisons_des_craintes" TEXT,
  "attentes_experimentation" TEXT,
  "autres_cas_usage_transverse" TEXT,
  "cas_d_usage_metier" TEXT,
  "formation_suivie_usage_ia_" TEXT,
  "autre_formation_suivie" TEXT,
  "autre_besoin_accompagnement" TEXT,
  "besoin_acculturation_encadrement" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE assistant_ia."dim_q2"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" text,
  "autres_types_d_interactions" TEXT,
  "id_niveau_d_usage_ia_post_expe_" INTEGER,
  "niveau_d_usage_ia" TEXT,
  "niveau_usage_ia" TEXT,
  "frequence_d_usage_assistant_ia" TEXT,
  "autres_formation_ia" TEXT,
  "raison_non_participation_rdv" TEXT,
  "autre_besoin_accompagnement" TEXT,
  "apprentissage_assistant_ia_ressenti_" TEXT,
  "difficultes_techniques_rencontrees2" TEXT,
  "id_recommandation_collegues_mef" INTEGER,
  "recommandation_collegues_mef" TEXT,
  "id_sensation_montee_en_competences" INTEGER,
  "sensation_montee_en_competences" TEXT,
  "id_evolution_des_craintes_initiales" INTEGER,
  "evolution_des_craintes_initiales" TEXT,
  "id_utilite_metier_mef" INTEGER,
  "utilite_metier_mef" TEXT,
  "decouverte_d_usages_inattendus" TEXT,
  "mode_de_decouverte_usages" TEXT,
  "id_diminution_d_usage_ia_non_souveraines" INTEGER,
  "diminution_d_usage_ia_non_souveraines" TEXT,
  "id_comparaison_autres_ia" INTEGER,
  "comparaison_autre_ia" TEXT,
  "frequence_des_erreurs" TEXT,
  "temps_economise_par_semaine" TEXT,
  "cu1_nombre_echanges_moyens_affinage_reponse" TEXT,
  "id_taux_moyen_de_correction_rep_assistant" INTEGER,
  "cu1_taux_moyen_de_correction_rep_assistant" TEXT,
  "pertinence_assistant_ia" TEXT,
  "commentaires" TEXT,
  "deuxieme_cas_d_usage_teste" TEXT,
  "cu2_temps_economise_par_semaine" TEXT,
  "cu2_nombre_echanges_moyens" TEXT,
  "id_cu2_taux_moyen_de_correction_rep_assistant" INTEGER,
  "cu2_taux_moyen_de_correction_rep_assistant" TEXT,
  "cu2_pertinence_assistant_ia" TEXT,
  "commentaires2" TEXT,
  "troisieme_cas_d_usage" TEXT,
  "cu3_temps_economise_par_semaine" TEXT,
  "cu3_nombre_echanges_moyens_affinage_reponse" TEXT,
  "id_cu3_taux_moyen_de_correction_rep_assistant" INTEGER,
  "cu3_taux_moyen_de_correction_rep_assistant" TEXT,
  "commentaires3" TEXT,
  "impact_sur_le_temps_de_travail" TEXT,
  "estimation_globale_gain_de_temps" TEXT,
  "id_raisons_perte_de_temps" INTEGER,
  "raisons_perte_de_temps" TEXT,
  "id_ia_favorise_relations_humaines_" INTEGER,
  "ia_favorise_relations_humaines" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE assistant_ia."dim_q3"(
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "id" int,
  "no_id" text,
  "temps_fonction_exercee" TEXT,
  "genre" TEXT,
  "frequence_utilisation" TEXT,
  "evolution_usage" TEXT,
  "quelles_raisons_facons" TEXT,
  -- Références simples (Clés étrangères)
  "id_raisons_non_participation" INTEGER,
  "raisons_non_participation" TEXT,
  "id_impacts_taches_pro" INTEGER,
  "impacts_taches_pro" TEXT,
  "id_impacts_taches_rebarbatives" INTEGER,
  "impacts_taches_rebarbatives" TEXT,
  "id_comparaison_autres_ia" INTEGER,
  "comparaisons_autres_ia" TEXT,
  "id_utilisation_moindre" INTEGER,
  "utilisation_moindre" TEXT,
  "id_recommandations" INTEGER,
  "recommandations" TEXT,
  -- Réponses textuelles libres et commentaires
  "autres_outils" TEXT,
  "satisfaction_autre_outil" TEXT,
  "autres" TEXT,
  "evaluation_niveau_acculturation" TEXT,
  "evolution_sentiment" TEXT,
  "autres_leviers" TEXT,
  "temps_gagnes" TEXT,
  "impact_perception" TEXT,
  "sentiment_de_fierte" TEXT,
  "experimentation_interne" TEXT,
  "autres_fonctionnalites" TEXT,
  "autres_risques" TEXT,
  "etre_ambassadeur" TEXT,
  "bonnes_pratiques" TEXT,
  "autres_besoins_importants" TEXT,
  "autres_besoins_moindres" TEXT,
  "ameliorations" TEXT,
  "aspects_a_ameliorer" TEXT,
  "interface" TEXT,
  "contenu" TEXT,
  "connexions" TEXT,
  "autre_retour_libre" TEXT,
  "retours_libres" TEXT,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
    UNIQUE ("import_timestamp", "no_id")
) PARTITION BY RANGE (import_timestamp);
