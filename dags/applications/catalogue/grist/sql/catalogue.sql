DROP SCHEMA documentation CASCADE;
CREATE SCHEMA documentation;

/*
    Référentiels
*/

CREATE TABLE documentation."ref_informationsystem" (
  "id" integer PRIMARY KEY,
  "si" text,
  "commentaire" text
);

CREATE TABLE documentation."ref_organisation" (
  "id" integer PRIMARY KEY,
  "nom" text,
  "commentaire" text,
  "siret" text
);

CREATE TABLE documentation."ref_service" (
  "id" integer PRIMARY KEY,
  "nom" text,
  "acronyme" text,
  "id_organisation" int,
	FOREIGN KEY ("id_direction") REFERENCES documentation."ref_organisation" ("id")
);

CREATE TABLE documentation."ref_people" (
  "id" integer PRIMARY KEY,
  "contact" text,
  "mail" text,
  "commentaire" text,
  "id_service" int,
	"profil" text,
	FOREIGN KEY ("id_service") REFERENCES documentation."ref_service" ("id")
);

CREATE TABLE documentation."ref_format" (
  "id" integer PRIMARY KEY,
  "format" text
);

CREATE TABLE documentation."ref_licence" (
  "id" integer PRIMARY KEY,
  "licence" text,
  "id_technique" text
);

CREATE TABLE documentation."ref_frequency" (
  "id" integer PRIMARY KEY,
  "frequence" text,
  "id_technique" text
);

CREATE TABLE documentation."ref_geographicalcoverage" (
  "id" integer PRIMARY KEY,
  "couverture_geographique" text,
  "id_technique" text
);

CREATE TABLE documentation."ref_theme" (
  "id" integer PRIMARY KEY,
  "theme" text
);

CREATE TABLE documentation."ref_contactpoint" (
  "id" integer PRIMARY KEY,
  "nom_bureau" text,
  "mail" text,
  "commentaire" text,
  "id_service" int,
	FOREIGN KEY ("id_service") REFERENCES documentation."ref_service" ("id")
);

CREATE TABLE documentation."ref_catalogue" (
  "id" integer PRIMARY KEY,
  "dataset_id" text
);

CREATE TABLE documentation."ref_typedonnees" (
  "id" integer PRIMARY KEY,
  "valeur" text
);

/*
    Données
*/

CREATE TABLE documentation."catalogue" (
  "id" integer PRIMARY KEY,
  "title" text,
  "description" text,
  "keyword" text[],
  "is_public" boolean,
  "id_structure" int,
  "id_service" int,
  "id_system_information" int,
  "id_contactpoint" int,
  "issued" date,
  "modified" date,
  "id_frequency" int,
  "id_geographicalcoverage" int,
  "url" text,
  "id_format" int[],
  "id_licence" int,
  "siret_organisation" text,
  "id_theme" int[],
  "donnees_ouvertes" boolean,
  "url_open_data" text,
  "volumetrie_en_mo_" numeric,
  "mail_contact_service" text,
  "table_name" text,
  "schema_name" text,
  "temporal" text,
  "updated_at" datetime,
  "est_visible" boolean,
	FOREIGN KEY ("id_structure") REFERENCES documentation."ref_organisation" ("id"),
	FOREIGN KEY ("id_service") REFERENCES documentation."ref_service" ("id"),
	FOREIGN KEY ("id_system_information") REFERENCES documentation."ref_informationsystem" ("id"),
	FOREIGN KEY ("id_contact_point") REFERENCES documentation."ref_contactpoint" ("id"),
	FOREIGN KEY ("id_frequency") REFERENCES documentation."ref_frequency" ("id"),
	FOREIGN KEY ("id_spatiale") REFERENCES documentation."ref_geographicalcoverage" ("id"),
	FOREIGN KEY ("id_format") REFERENCES documentation."ref_format" ("id"),
	FOREIGN KEY ("id_licence") REFERENCES documentation."ref_licence" ("id"),
	FOREIGN KEY ("id_theme") REFERENCES documentation."ref_theme" ("id")
);


CREATE TABLE documentation."dictionnaire" (
  "id" integer PRIMARY KEY,
  "id_jeu_de_donnees" int,
  "variable" text,
  "unite" text,
  "commentaire" text,
  "id_data_type" int,
  "row_created_at" date,
  "row_last_updated_at" date,
	FOREIGN KEY ("id_jeu_de_donnees") REFERENCES documentation."catalogue" ("id"),
	FOREIGN KEY ("id_data_type") REFERENCES documentation."ref_typedonnees" ("id")
);
