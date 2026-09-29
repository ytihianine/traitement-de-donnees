-- Create
DROP SCHEMA IF EXISTS conf_projets CASCADE;
CREATE SCHEMA IF NOT EXISTS conf_projets;

/*
  Référentiels
*/
DROP TABLE IF EXISTS conf_projets."ref_direction" CASCADE;
CREATE TABLE conf_projets."ref_direction" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_direction" int,
  "direction" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_direction", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."ref_service" CASCADE;
CREATE TABLE conf_projets."ref_service" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_service" int,
  "id_direction" int,
  "service" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_service", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."ref_type_location" CASCADE;
CREATE TABLE conf_projets."ref_type_location" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_type_location" int,
  "type_location" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_type_location", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."ref_connexion" CASCADE;
CREATE TABLE conf_projets."ref_connexion" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_connexion" int,
  "id_type_location" int,
  "conn_id" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_type_location", "id_connexion", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


/*
  Tables métiers
*/
DROP TABLE IF EXISTS conf_projets."projet" CASCADE;
CREATE TABLE conf_projets."projet" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_direction" int,
  "id_service" int,
  "projet" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_projet", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."projet_location" CASCADE;
CREATE TABLE conf_projets."projet_location" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "bucket" text,
  "fs_folder" text,
  "fs_folder_tmp" text,
  "db_schema" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_projet", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."projet_documentation" CASCADE;
CREATE TABLE conf_projets."projet_documentation" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_documentation" int,
  "type_documentation" text,
  "lien" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_projet", "id_documentation", "import_timestamp"),
  UNIQUE ("id_projet", "type_documentation", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."projet_contact" CASCADE;
CREATE TABLE conf_projets."projet_contact" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_contact" int,
  "contact_mail" text,
  "is_mail_generic" bool,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_projet", "id_contact", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."dataset" CASCADE;
CREATE TABLE conf_projets."dataset" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_dataset" int,
  "id_projet" int,
  "dataset" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_projet", "id_dataset", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."dataset_location" CASCADE;
CREATE TABLE conf_projets."dataset_location" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "id_dataset" int,
  "stage" text,
  "id_type_location" int,
  "location" text,
  "id_conn_id" int,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_projet", "id_dataset", "stage", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."dataset_column_mapping" CASCADE;
CREATE TABLE conf_projets."dataset_column_mapping" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_col_mapping" int,
  "id_projet" int,
  "id_dataset" int,
  "colname_source" text,
  "colname_dest" text,
  "to_keep" bool,
  "date_archivage" date,
  "import_timestamp" TIMESTAMP NOT NULL,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp"),
  UNIQUE ("id_projet", "id_dataset", "id_col_mapping", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


/*
  Tables des faits
*/
-- Dimensions pour les projets
DROP TABLE IF EXISTS conf_projets."dim_projet" CASCADE;
CREATE TABLE conf_projets."dim_projet" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "projet" text,
  "id_direction" int,
  "direction" text,
  "id_service" int,
  "service" text,
  "bucket" text,
  "fs_folder" text,
  "fs_folder_tmp" text,
  "db_schema" text,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  "import_timestamp" TIMESTAMP NOT NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."dim_projet_contact" CASCADE;
CREATE TABLE conf_projets."dim_projet_contact" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "projet" text,
  "id_contact" int,
  "contact_mail" text,
  "is_mail_generic" boolean,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  "import_timestamp" TIMESTAMP NOT NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS conf_projets."dim_projet_documentation" CASCADE;
CREATE TABLE conf_projets."dim_projet_documentation" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "projet" text,
  "id_documentation" int,
  "type_documentation" text,
  "lien" text,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  "import_timestamp" TIMESTAMP NOT NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

-- Dimensions pour les datasets
DROP TABLE IF EXISTS conf_projets."dim_dataset" CASCADE;
CREATE TABLE conf_projets."dim_dataset" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "projet" text,
  "id_direction" int,
  "direction" text,
  "id_service" int,
  "service" text,
  "id_dataset" int,
  "dataset" text,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  "import_timestamp" TIMESTAMP NOT NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."dim_dataset_location" CASCADE;
CREATE TABLE conf_projets."dim_dataset_location" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "projet" text,
  "id_dataset" int,
  "dataset" text,
  "stage" text,
  "id_type_location" int,
  "type_location" text,
  "location" text,
  "id_conn_id" int,
  "conn_id" text,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  "import_timestamp" TIMESTAMP NOT NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS conf_projets."dim_dataset_column_mapping" CASCADE;
CREATE TABLE conf_projets."dim_dataset_column_mapping" (
  "id_row" bigint GENERATED ALWAYS AS IDENTITY,
  "id_projet" int,
  "projet" text,
  "id_dataset" int,
  "dataset" text,
  "id_col_mapping" int,
  "colname_source" text,
  "colname_dest" text,
  "to_keep" boolean,
  "date_archivage" date,
  "snapshot_id" UUID NOT NULL,
  "snapshot_id_parent" UUID NULL,
  "import_timestamp" TIMESTAMP NOT NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


-- [TO REFACTOR] vue_source pour l'interface de dépôt de fichier
drop view IF EXISTS conf_projets.vue_source;
create or replace
view conf_projets.vue_source
as
select
	cpp.snapshot_id,
	cpps.id_selecteur as "id",
	cpp.projet as "nom_projet",
	cpps.selecteur,
	cpps3.bucket as "s3_bucket",
	cpps3.key as "s3_key",
	cppsource.id_source as "nom_source",
	cpp.id_direction,
	cp_ref_dir.direction,
	cpp.id_service,
	cp_ref_service.service
from
	conf_projets.projet cpp
inner join conf_projets.projet_s3 cpps3
  on
	cpp.id_projet = cpps3.id_projet
	and cpp.snapshot_id = cpps3.snapshot_id
inner join conf_projets.projet_selecteur cpps
  on
	cpp.id_projet = cpps.id_projet
	and cpp.snapshot_id = cpps.snapshot_id
join conf_projets.selecteur_source cppsource
  on
	cpp.id_projet = cppsource.id_projet
	and cpps.id_selecteur = cppsource.id_selecteur
	and cpp.snapshot_id = cppsource.snapshot_id
	and cppsource.type_location = 'Fichier'
join conf_projets.ref_direction cp_ref_dir
  on
	cpp.id_direction = cp_ref_dir.id_direction
	and cpp.snapshot_id = cp_ref_dir.snapshot_id
join conf_projets.ref_service cp_ref_service
  on
	cpp.id_service = cp_ref_service.id_service
	and cpp.snapshot_id = cp_ref_service.snapshot_id
	-- Conserver uniquement la dernière configuration
where
	cpp.import_timestamp = (
	select
		MAX(import_timestamp)
	from
		conf_projets.projet
	limit 1
)
order by
	cpp.id_projet,
	cpp.import_timestamp desc
;
