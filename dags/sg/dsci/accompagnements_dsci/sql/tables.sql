DROP SCHEMA IF EXISTS activite_dsci CASCADE;
CREATE SCHEMA IF NOT EXISTS activite_dsci;

/*
    Référentiels
*/

CREATE TABLE activite_dsci."ref_typologie_accompagnement" (
  "id" integer PRIMARY KEY,
  "typologie_accompagnement" text
);

CREATE TABLE activite_dsci."ref_bureau" (
  "id" integer PRIMARY KEY,
  "bureau" text
);

drop table activite_dsci."ref_profil_correspondant" CASCade;
CREATE TABLE activite_dsci."ref_profil_correspondant" (
  "id" integer PRIMARY KEY,
  "profil_correspondant" text,
  "intitule_long" text,
  "created_by" text,
  "updated_by" text
  -- "created_at" date,
  -- "updated_at" date
);

CREATE TABLE activite_dsci."ref_direction" (
  "id" integer PRIMARY KEY,
  "direction" text,
  "libelle_long" text,
  "administration" text
);

CREATE TABLE activite_dsci."ref_region" (
  "id" integer PRIMARY KEY,
  "region" text
);

CREATE TABLE activite_dsci."ref_certification" (
  "id" integer PRIMARY KEY,
  "competence" text
);

CREATE TABLE activite_dsci."ref_pole" (
  "id" integer PRIMARY KEY,
  "id_bureau" int,
  "pole" text,
	FOREIGN KEY ("id_bureau") REFERENCES activite_dsci."ref_bureau" ("id")
);

CREATE TABLE activite_dsci."ref_type_accompagnement" (
  "id" integer PRIMARY KEY,
  "type_d_accompagnement" text,
  "id_pole" int,
	FOREIGN KEY ("id_pole") REFERENCES activite_dsci."ref_pole" ("id")
);

DROP TABLE activite_dsci."ref_semainier";
CREATE TABLE activite_dsci."ref_semainier" (
  "id" integer PRIMARY KEY,
  "annee" int,
  "mois" text,
  "trimestre" text,
  "date_semaine" date,
  "semaine" int
);

CREATE TABLE activite_dsci."ref_qualite_service" (
  "id" integer PRIMARY KEY,
  "qualite_de_service" text
);


CREATE TABLE activite_dsci."ref_competence_particuliere" (
  "id" integer PRIMARY KEY,
  "competence" text
);



/*
    Données
*/
DROP TABLE activite_dsci."correspondant" CASCADE;
CREATE TABLE activite_dsci."correspondant" (
	"id" integer PRIMARY KEY,
	"mail" text,
	"nom_complet" text,
	"id_direction" int,
	"entite" text,
	"id_region" int,
	"actif" boolean,
	"id_promotion_fac" int,
	"est_certifie_fac" boolean,
	"actif_communaute_fac" boolean,
	"direction_hors_mef" text,
	-- "fac_certifications_realisees" text,
	-- "type_correspondant_text" text,
	"prenom" text,
	"nom" text,
	-- "created_by" text,
	-- "created_at" timestamp,
	-- "updated_at" timestamp,
	-- "updated_by" text,
	"date_debut_inactivite" date,
	-- "is_duplicate" grist_any,
	-- "check_mail" grist_any,
	-- "poste" text,
	-- "cause_inactivite" text,
	FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id"),
	FOREIGN KEY ("id_region") REFERENCES activite_dsci."ref_region" ("id"),
	FOREIGN KEY ("id_promotion_fac") REFERENCES activite_dsci."accompagnement_mi" ("id")
);

DROP TABLE activite_dsci."correspondant_profil" CASCADE;
CREATE TABLE activite_dsci."correspondant_profil" (
	"id_correspondant" integer,
	"id_type_de_correspondant" int,
    PRIMARY KEY ("id_correspondant", "id_type_de_correspondant"),
	FOREIGN KEY ("id_correspondant") REFERENCES activite_dsci."correspondant" ("id"),
	FOREIGN KEY ("id_type_de_correspondant") REFERENCES activite_dsci."ref_profil_correspondant" ("id")
);

CREATE TABLE activite_dsci."correspondant_competence_particuliere" (
	"id_correspondant" integer PRIMARY KEY,
	"id_competence_particuliere" int,
	FOREIGN KEY ("id_correspondant") REFERENCES activite_dsci."correspondant" ("id"),
	FOREIGN KEY ("id_competence_particuliere") REFERENCES activite_dsci."ref_competence_particuliere" ("id")
);

CREATE TABLE activite_dsci."correspondant_connaissance_communaute" (
	"id_correspondant" integer PRIMARY KEY,
	"connaissance_communaute" text,
	FOREIGN KEY ("id_correspondant") REFERENCES activite_dsci."correspondant" ("id")
);

DROP TABLE activite_dsci."accompagnement_mi" CASCADE;
CREATE TABLE activite_dsci."accompagnement_mi" (
	"id" integer PRIMARY KEY,
	"est_ouvert_notation" boolean,
	-- "quest_passinnov_inscription" text,
	-- "certifications_possibiles_txt" grist_any,
	-- "quest_passinnov_satisfaction" text,
	-- "quest_formationfac_satisfaction" grist_any,
	-- "quest_pleniere_inscription" grist_any,
	-- "quest_formation_codev_inscription" grist_any,
	-- "lieu" text,
	-- "id_certifications_possibles" int[],
	"est_certifiant" boolean,
	-- "canal_de_communication" grist_any,
	"places_max" int,
	"nb_inscrits" int,
	"places_restantes" int,
	-- "type_accompagnement_text" text,
	-- "direction_direction" text,
	"intitule" text,
	"id_direction" int,
	"date_de_realisation" date,
	"statut" text,
	"id_pole" int,
	"id_type_d_accompagnement" int,
	"informations_complementaires" text,
--ALTER TABLE "accompagnement_mi" ADD FOREIGN KEY ("id_certifications_possibles") REFERENCES activite_dsci."ref_certification" ("id");
	FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id"),
	FOREIGN KEY ("id_pole") REFERENCES activite_dsci."ref_pole" ("id"),
	FOREIGN KEY ("id_type_d_accompagnement") REFERENCES activite_dsci."ref_type_accompagnement" ("id")
);

CREATE TABLE activite_dsci."accompagnement_mi_satisfaction" (
	"id" integer PRIMARY KEY,
	"id_accompagnement" int,
	"nombre_de_participants" numeric,
	"nombre_de_reponses" numeric,
	"taux_de_reponse" numeric,
	"note_moyenne_de_satisfaction" numeric,
	"unite" text,
	"id_type_d_accompagnement" int,
	FOREIGN KEY ("id_accompagnement") REFERENCES activite_dsci."accompagnement_mi" ("id"),
	FOREIGN KEY ("id_type_d_accompagnement") REFERENCES activite_dsci."ref_type_accompagnement" ("id")
);

CREATE TABLE activite_dsci."bilaterale" (
  "id" integer PRIMARY KEY,
  "id_direction" int,
  "date_de_rencontre" date,
  "intitule" text
);

CREATE TABLE activite_dsci."bilaterale_remontee" (
	"id" integer PRIMARY KEY,
	"id_bilaterale" int,
	"id_bureau" int,
	"information_a_remonter" text,
	-- "id_int_direction" int,
	FOREIGN KEY ("id_bilaterale") REFERENCES activite_dsci."bilaterale" ("id"),
	FOREIGN KEY ("id_bureau") REFERENCES activite_dsci."ref_bureau" ("id")
);

-- To do : add the following tables




CREATE TABLE activite_dsci."accompagnement_dsci" (
  "id" integer PRIMARY KEY,
  "annee" numeric,
  "statut" text,
  "id_typologie" int[],
  "prestataire" text,
  "commentaires_complements" text,
  "ressources_documentaires" text,
  "debut_previsionnel_de_l_accompagnement" date,
  "fin_previsionnelle_de_l_accompagnement" date,
  "intitule_de_l_accompagnement" text,
  "autres_participants" text,
  "id_direction2" int,
  "service_bureau" text,
  "sous_dir_bureau_" text,
  "id_equipe_s_dsci" int[],
  "id_porteur_dsci" int[],
  "nom_du_prestataire" text,
  "equipe_dsci_txt" text,
  "formulaire_cci" text,
  "date_de_cloture_questionnaire" date,
  "porteur_metier" text,
ALTER TABLE "accompagnement_dsci" ADD FOREIGN KEY ("id_typologie") REFERENCES activite_dsci."ref_typologie_accompagnement" ("id");
ALTER TABLE "accompagnement_dsci" ADD FOREIGN KEY ("id_direction2") REFERENCES activite_dsci."ref_direction" ("id");
ALTER TABLE "accompagnement_dsci" ADD FOREIGN KEY ("id_equipe_s_dsci") REFERENCES activite_dsci."ref_bureau" ("id");
ALTER TABLE "accompagnement_dsci" ADD FOREIGN KEY ("id_porteur_dsci") REFERENCES activite_dsci."effectif_dsci" ("id");
);



CREATE TABLE activite_dsci."effectif_dsci" (
  "id" integer PRIMARY KEY,
  "mail" text,
  "id_bureau" int,
  "id_pole" int,
  "bureau_texte" text,
  "nom_complet" text,
  "agent_present" boolean,
  "fonction" text,
  "created_at" text,
  "updated_at" text,
  "created_by" text,
  "updated_by" text,
  "absent_depuis" date,
ALTER TABLE "effectif_dsci" ADD FOREIGN KEY ("id_bureau") REFERENCES activite_dsci."ref_bureau" ("id");
ALTER TABLE "effectif_dsci" ADD FOREIGN KEY ("id_pole") REFERENCES activite_dsci."ref_pole" ("id");
);



CREATE TABLE activite_dsci."animateur_interne" (
  "id" integer PRIMARY KEY,
  "id_accompagnement" int,
  "id_animateur" int
);

CREATE TABLE activite_dsci."animateur_externe" (
  "id" integer PRIMARY KEY,
  "id_accompagnement" int,
  "animateur" text
);

CREATE TABLE activite_dsci."animateur_fac" (
  "id" integer PRIMARY KEY,
  "id_accompagnement" int,
  "id_certifications_validees" int[],
  "cert_possibles_txt" text,
  "cert_validees_txt" text,
  "cert_souhaitees_txt" text,
  "id_certifications_souhaitees" int[],
  "id_animateur" int,
ALTER TABLE "animateur_fac" ADD FOREIGN KEY ("id_accompagnement") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "animateur_fac" ADD FOREIGN KEY ("id_certifications_validees") REFERENCES activite_dsci."ref_certification" ("id");
ALTER TABLE "animateur_fac" ADD FOREIGN KEY ("id_certifications_souhaitees") REFERENCES activite_dsci."ref_certification" ("id");
ALTER TABLE "animateur_fac" ADD FOREIGN KEY ("id_animateur") REFERENCES activite_dsci."correspondant" ("id");
);

CREATE TABLE activite_dsci."charge_agent_cci" (
	"id" integer PRIMARY KEY,
	"trimestre" text,
	"type_de_charge" text,
	"equipe" grist_any,
	"id_missions" int,
	"id_semaine" int,
	"id_agent_e_" int,
	"temps_passe" numeric,
	"taux_de_charge" numeric,
	"annee" grist_any,
ALTER TABLE "charge_agent_cci" ADD FOREIGN KEY ("id_missions") REFERENCES activite_dsci."accompagnement_dsci" ("id");
ALTER TABLE "charge_agent_cci" ADD FOREIGN KEY ("id_semaine") REFERENCES activite_dsci."ref_semainier" ("id");
ALTER TABLE "charge_agent_cci" ADD FOREIGN KEY ("id_agent_e_") REFERENCES activite_dsci."effectif_dsci" ("id");
);



CREATE TABLE activite_dsci."accompagnement_opportunite_cci" (
	"id" integer PRIMARY KEY,
	"date_prise_de_decision" date,
	"date_de_proposition_d_accompagnement" date,
	"decision" text,
	"date_de_reception" date,
	"id_accompagnement" int,
	"expression_de_besoin_transmise" boolean,
	"type_de_canal" text,
	"statut" text,
	"convention_d_accompagnement" boolean,
	"commentaires" text,
	"precision_canal" text,
	"proposition_d_accompagnement_transmise" boolean,
ALTER TABLE "accompagnement_opportunite_cci" ADD FOREIGN KEY ("id_accompagnement") REFERENCES activite_dsci."accompagnement_dsci" ("id");

);



CREATE TABLE activite_dsci."accompagnement_cci_quest_satisfaction" (
	"id" integer PRIMARY KEY,
	"appreciation_globale" text,
	"points_d_ameliorations" text,
	"points_forts" text,
	"id_adaptabilite" int,
	"id_formulaire_accompagnement" int,
	"id_relationnel_client" int,
	"id_qualite_des_livrables" int,
	"id_atteinte_objectifs" int,
	"score_de_recommandation" text,
	"id_pilotage_et_suivi" int,
	"autres_elements" text,
	"id_etape_de_cadrage" int,
	"id_aide_methodologique" int,
	"id_reactivite" int,
	"id_respect_calendrier" int,
	"mail" text,
	"id_accompagnement" int,

ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_adaptabilite") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_formulaire_accompagnement") REFERENCES activite_dsci."accompagnement_dsci" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_relationnel_client") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_qualite_des_livrables") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_atteinte_objectifs") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_pilotage_et_suivi") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_etape_de_cadrage") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_aide_methodologique") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_reactivite") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_respect_calendrier") REFERENCES activite_dsci."ref_qualite_service" ("id");
ALTER TABLE "accompagnement_cci_quest_satisfaction" ADD FOREIGN KEY ("id_accompagnement") REFERENCES activite_dsci."accompagnement_dsci" ("id");
);



CREATE TABLE activite_dsci."fac_hors_bercylab_quest_accompagnement" (
	"id" integer PRIMARY KEY,
	"id_facilitateur_1" int,
	"id_facilitateur_2" int,
	"id_facilitateur_3" int,
	"id_facilitateurs" int[],
	"id_direction" int,
	"synthese_de_l_accompagnement" text,
	"id_region" int,
	"date_de_realisation" date,
	"type_d_accompagnement" text[],
	"intitule_de_l_accompagnement" text,
	"statut" text,
	"participants" text[]
);

CREATE TABLE activite_dsci."passinnov_quest_inscription" (
	"id" integer PRIMARY KEY,
	"is_duplicate" numeric,
	"id_region" int,
	"mail" text,
	"id_direction" int,
	"id_passinnov" int,
	"id_id_accompagnement" int,
	"role" text
);

CREATE TABLE activite_dsci."passinnov_quest_satisfaction" (
	"id" integer PRIMARY KEY,
	"is_duplicate" grist_any,
	"mail" text,
	"id_id_passinnov" int,
	"commentaires" text,
	"id_quest_passinnov" int,
	"note_globale" text
);

CREATE TABLE activite_dsci."formation_fac_quest_satisfaction" (
	"id" integer PRIMARY KEY,
	"note_module_2" text,
	"id_promotion" int,
	"mail" text,
	"envies_pour_la_suite" text[],
	"besoin" text,
	"nps" text,
	"utilite" text,
	"id_quest_formation" int,
	"commentaire_m2" text,
	"commentaire_m1" text,
	"note_module_1" text,
	"commentaire_m3" text,
	"id_id_formation" int,
	"note_module_3" text
);

CREATE TABLE activite_dsci."correspondant_maj" (
	"id" integer PRIMARY KEY,
	"id_region" int,
	"id_type_de_correspondant" int[],
	"prenom" text,
	"nom" text,
	"souhait_certification2" text,
	"competence_autre2" grist_any,
	"id_promotion" int[],
	"id_direction" int,
	"mail" text,
	"nom_complet" text,
	"id_competence_particuliere" int[],
	"poste" text,
	"cause_inactivite" text,
	"type_correspondant_text" text,
	"connaissance_communaute" text[],
	"actif" text,
	"direction_hors_mef2" text,
	"entite" text
);

CREATE TABLE activite_dsci."laboratoires_territoriaux" (
	"id" integer PRIMARY KEY,
	"nom" grist_any,
	"id_direction" int,
	"id_region" int
);

CREATE TABLE activite_dsci."formation_codev_quest_inscription" (
	"id" integer PRIMARY KEY,
	"attentes" text,
	"difficultes" text,
	"mail" text,
	"formation_codev" text,
	"id_session_formation_codev" int,
	"is_duplicate" grist_any,
	"id_direction" int,
	"experience_codev" text,
	"details_experience" text,
	"id_id_accompagnement" int
);

CREATE TABLE activite_dsci."pleniere_quest_inscription" (
	"id" integer PRIMARY KEY,
	"is_duplicate" grist_any,
	"id_direction" int,
	"mail" text,
	"id_pleniere" int,
	"id_id_accompagnement" int
);




-- TO DO: Intégrer ces FOREIGN KEYS dans leurs tables respectives




ALTER TABLE "bilaterale" ADD FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id");



ALTER TABLE "animateur_interne" ADD FOREIGN KEY ("id_accompagnement") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "animateur_interne" ADD FOREIGN KEY ("id_animateur") REFERENCES activite_dsci."effectif_dsci" ("id");

ALTER TABLE "animateur_externe" ADD FOREIGN KEY ("id_accompagnement") REFERENCES activite_dsci."accompagnement_mi" ("id");


ALTER TABLE "fac_hors_bercylab_quest_accompagnement" ADD FOREIGN KEY ("id_facilitateur_1") REFERENCES activite_dsci."correspondant" ("id");
ALTER TABLE "fac_hors_bercylab_quest_accompagnement" ADD FOREIGN KEY ("id_facilitateur_2") REFERENCES activite_dsci."correspondant" ("id");
ALTER TABLE "fac_hors_bercylab_quest_accompagnement" ADD FOREIGN KEY ("id_facilitateur_3") REFERENCES activite_dsci."correspondant" ("id");
ALTER TABLE "fac_hors_bercylab_quest_accompagnement" ADD FOREIGN KEY ("id_facilitateurs") REFERENCES activite_dsci."correspondant" ("id");
ALTER TABLE "fac_hors_bercylab_quest_accompagnement" ADD FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id");
ALTER TABLE "fac_hors_bercylab_quest_accompagnement" ADD FOREIGN KEY ("id_region") REFERENCES activite_dsci."ref_region" ("id");

ALTER TABLE "passinnov_quest_inscription" ADD FOREIGN KEY ("id_region") REFERENCES activite_dsci."ref_region" ("id");
ALTER TABLE "passinnov_quest_inscription" ADD FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id");
ALTER TABLE "passinnov_quest_inscription" ADD FOREIGN KEY ("id_passinnov") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "passinnov_quest_inscription" ADD FOREIGN KEY ("id_id_accompagnement") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "passinnov_quest_satisfaction" ADD FOREIGN KEY ("id_id_passinnov") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "passinnov_quest_satisfaction" ADD FOREIGN KEY ("id_quest_passinnov") REFERENCES activite_dsci."accompagnement_mi" ("id");

ALTER TABLE "formation_fac_quest_satisfaction" ADD FOREIGN KEY ("id_promotion") REFERENCES activite_dsci."ref_promotion_fac" ("id");
ALTER TABLE "formation_fac_quest_satisfaction" ADD FOREIGN KEY ("id_quest_formation") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "formation_fac_quest_satisfaction" ADD FOREIGN KEY ("id_id_formation") REFERENCES activite_dsci."accompagnement_mi" ("id");

ALTER TABLE "correspondant_maj" ADD FOREIGN KEY ("id_region") REFERENCES activite_dsci."ref_region" ("id");
ALTER TABLE "correspondant_maj" ADD FOREIGN KEY ("id_type_de_correspondant") REFERENCES activite_dsci."ref_profil_correspondant" ("id");
ALTER TABLE "correspondant_maj" ADD FOREIGN KEY ("id_promotion") REFERENCES activite_dsci."correspondant_maj" ("id");
ALTER TABLE "correspondant_maj" ADD FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id");
ALTER TABLE "correspondant_maj" ADD FOREIGN KEY ("id_competence_particuliere") REFERENCES activite_dsci."ref_competence_particuliere" ("id");

ALTER TABLE "laboratoires_territoriaux" ADD FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id");
ALTER TABLE "laboratoires_territoriaux" ADD FOREIGN KEY ("id_region") REFERENCES activite_dsci."ref_region" ("id");

ALTER TABLE "formation_codev_quest_inscription" ADD FOREIGN KEY ("id_session_formation_codev") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "formation_codev_quest_inscription" ADD FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id");
ALTER TABLE "formation_codev_quest_inscription" ADD FOREIGN KEY ("id_id_accompagnement") REFERENCES activite_dsci."accompagnement_mi" ("id");

ALTER TABLE "pleniere_quest_inscription" ADD FOREIGN KEY ("id_direction") REFERENCES activite_dsci."ref_direction" ("id");
ALTER TABLE "pleniere_quest_inscription" ADD FOREIGN KEY ("id_pleniere") REFERENCES activite_dsci."accompagnement_mi" ("id");
ALTER TABLE "pleniere_quest_inscription" ADD FOREIGN KEY ("id_id_accompagnement") REFERENCES activite_dsci."accompagnement_mi" ("id");
