-- Create
CREATE SCHEMA IF NOT EXISTS conf_projets;


DROP TABLE conf_projets."ref_direction" CASCADE;
CREATE TABLE conf_projets."ref_direction" (
  "id" int,
  "direction" text,
  PRIMARY KEY ("id"),
  UNIQUE ("direction")
);


CREATE TABLE conf_projets."ref_service" (
  "id" int,
  "id_direction" int,
  "service" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_direction") REFERENCES conf_projets."ref_direction" ("id"),
  UNIQUE ("service")
);


DROP TABLE conf_projets."projet" CASCADE;
CREATE TABLE conf_projets."projet" (
  "id" int,
  "id_direction" int,
  "id_service" int,
  "projet" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_direction") REFERENCES conf_projets."ref_direction" ("id"),
  FOREIGN KEY ("id_service") REFERENCES conf_projets."ref_service" ("id"),
  UNIQUE ("id_direction", "id_service", "projet")
);


DROP TABLE conf_projets."projet_documentation";
CREATE TABLE conf_projets."projet_documentation" (
  "id" int,
  "id_projet" int,
  "type_documentation" text,
  "lien" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  UNIQUE ("id_projet", "type_documentation")
);

DROP TABLE conf_projets."projet_s3";
CREATE TABLE conf_projets."projet_s3" (
  "id" int,
  "id_projet" int,
  "bucket" text,
  "key" text,
  "key_tmp" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  UNIQUE ("id_projet")
);


DROP TABLE conf_projets."projet_contact";
CREATE TABLE conf_projets."projet_contact" (
  "id" int,
  "id_projet" int,
  "contact_mail" text,
  "is_mail_generic" bool,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  UNIQUE ("id_projet", "contact_mail")
);

DROP TABLE conf_projets."projet_selecteur";
CREATE TABLE conf_projets."projet_selecteur" (
  "id" int,
  "id_projet" int,
  "type_selecteur" text,
  "selecteur" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  UNIQUE ("id_projet", "selecteur")
);

DROP TABLE conf_projets."selecteur_source";
CREATE TABLE conf_projets."selecteur_source" (
  "id" int,
  "id_projet" int,
  "id_selecteur" int,
  "type_source" text,
  "id_source" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  FOREIGN KEY ("id_selecteur") REFERENCES conf_projets."projet_selecteur" ("id"),
  UNIQUE ("id_projet", "id_selecteur", "type_source", "id_source")
);


DROP TABLE conf_projets."selecteur_s3";
CREATE TABLE conf_projets."selecteur_s3" (
  "id" int,
  "id_projet" int,
  "id_selecteur" int,
  "key" text,
  "filename" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  FOREIGN KEY ("id_selecteur") REFERENCES conf_projets."projet_selecteur" ("id"),
  UNIQUE ("id_projet", "id_selecteur", "filename")
);


DROP TABLE conf_projets."selecteur_database";
CREATE TABLE conf_projets."selecteur_database" (
  "id" int,
  "id_projet" int,
  "id_selecteur" int,
  "tbl_name" text,
  "tbl_order" int,
  "is_partitionned" bool,
  "partition_period" text,
  "load_strategy" text,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  FOREIGN KEY ("id_selecteur") REFERENCES conf_projets."projet_selecteur" ("id"),
  UNIQUE ("id_projet", "id_selecteur", "tbl_name")
);


DROP TABLE conf_projets."selecteur_column_mapping";
CREATE TABLE conf_projets."selecteur_column_mapping" (
  "id" int,
  "id_projet" int,
  "id_selecteur" int,
  "colname_source" text,
  "colname_dest" text,
  "to_keep" bool,
  "date_archivage" date,
  PRIMARY KEY ("id"),
  FOREIGN KEY ("id_projet") REFERENCES conf_projets."projet" ("id"),
  FOREIGN KEY ("id_selecteur") REFERENCES conf_projets."projet_selecteur" ("id"),
  UNIQUE ("id_projet", "id_selecteur", "colname_source")
);


-- Vue pour get_projet_s3_info()
DROP VIEW conf_projets.projet_s3_vw;
CREATE OR REPLACE VIEW conf_projets.projet_s3_vw AS
SELECT
    cpp.projet,
    cpps.bucket,
    cpps.key,
    cpps.key_tmp
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_s3 cpps ON cpp.id = cpps.id_projet;


-- Vue pour selecteur_s3 avec fallback sur projet_s3
DROP VIEW conf_projets.selecteur_s3_vw;
CREATE OR REPLACE VIEW conf_projets.selecteur_s3_vw AS
SELECT
    cpp.projet,
    cpps.selecteur,
    cpss3.filename,
    COALESCE(cpss3.key, cpps3.key) as s3_key,
    cpps3.bucket,
    cpps3.key as projet_s3_key,
    cpps3.key_tmp as projet_s3_key_tmp,
    CONCAT(COALESCE(cpss3.key, cpps3.key), '/', cpss3.filename) as filepath_s3,
    CONCAT(cpps3.key_tmp, '/', cpss3.filename) as filepath_tmp_s3
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_selecteur cpps ON cpp.id = cpps.id_projet
INNER JOIN conf_projets.selecteur_s3 cpss3 ON cpps.id = cpss3.id_selecteur
    AND cpps.id_projet = cpss3.id_projet
INNER JOIN conf_projets.projet_s3 cpps3 ON cpp.id = cpps3.id_projet;

-- Vue pour les sources de type Grist
DROP VIEW conf_projets.selecteur_source_grist_vw;
CREATE OR REPLACE VIEW conf_projets.selecteur_source_grist_vw AS
SELECT
    cpp.projet,
    cpps.selecteur,
    cpss.type_source,
    cpss.id_source,
    cpss3.filename,
    COALESCE(cpss3.key, cpps3.key) as s3_key,
    cpps3.bucket,
    cpps3.key as projet_s3_key,
    cpps3.key_tmp as projet_s3_key_tmp,
    CONCAT(COALESCE(cpss3.key, cpps3.key), '/', cpss3.filename) as filepath_s3,
    CONCAT(cpps3.key_tmp, '/', cpss3.filename) as filepath_tmp_s3
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_s3 cpps3 ON cpp.id = cpps3.id_projet
INNER JOIN conf_projets.projet_selecteur cpps ON cpp.id = cpps.id_projet
INNER JOIN conf_projets.selecteur_s3 cpss3 ON cpps.id = cpss3.id_selecteur
    AND cpps.id_projet = cpss3.id_projet
INNER JOIN conf_projets.selecteur_source cpss ON cpps.id = cpss.id_selecteur
    AND cpps.id_projet = cpss.id_projet
WHERE cpss.type_source = 'Grist';

-- Vue pour les sources de type Fichier avec S3
DROP VIEW conf_projets.selecteur_source_fichier_vw;
CREATE OR REPLACE VIEW conf_projets.selecteur_source_fichier_vw AS
SELECT
    cpp.projet,
    cpps.selecteur,
    cpss.type_source,
    cpss.id_source,
    CONCAT(cpps3.key, '/', cpss.id_source) as filepath_source_s3,
    cpss3.filename,
    COALESCE(cpss3.key, cpps3.key) as s3_key,
    cpps3.bucket,
    cpps3.key as projet_s3_key,
    cpps3.key_tmp as projet_s3_key_tmp,
    CONCAT(COALESCE(cpss3.key, cpps3.key), '/', cpss3.filename) as filepath_s3,
    CONCAT(cpps3.key_tmp, '/', cpss3.filename) as filepath_tmp_s3
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_selecteur cpps
  ON cpp.id = cpps.id_projet
INNER JOIN conf_projets.selecteur_source cpss
  ON cpps.id = cpss.id_selecteur
  AND cpps.id_projet = cpss.id_projet
INNER JOIN conf_projets.projet_s3 cpps3 ON cpp.id = cpps3.id_projet
INNER JOIN conf_projets.selecteur_s3 cpss3 ON cpps.id = cpss3.id_selecteur
    AND cpps.id_projet = cpss3.id_projet
WHERE cpss.type_source = 'Fichier';

-- Vue pour column_mapping_dataframe()
DROP VIEW conf_projets.cols_mapping_vw;
CREATE OR REPLACE VIEW conf_projets.cols_mapping_vw AS
SELECT
    p.projet,
    ps.selecteur,
    scm.colname_source,
    scm.colname_dest
FROM conf_projets.projet p
INNER JOIN conf_projets.projet_selecteur ps ON p.id = ps.id_projet
INNER JOIN conf_projets.selecteur_column_mapping scm ON ps.id = scm.id_selecteur
    AND ps.id_projet = scm.id_projet
WHERE scm.to_keep = true;


-- Vue pour get_list_documentation()
DROP VIEW conf_projets.projet_documentation_vw;
CREATE OR REPLACE VIEW conf_projets.projet_documentation_vw AS
SELECT
    cpp.projet,
    cppd.type_documentation,
    cppd.lien
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_documentation cppd ON cpp.id = cppd.id_projet;

-- Vue pour get_list_contact()
DROP VIEW conf_projets.projet_contact_vw;
CREATE OR REPLACE VIEW conf_projets.projet_contact_vw AS
SELECT
    cpp.projet,
    cppc.contact_mail,
    cppc.is_mail_generic
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_contact cppc ON cpp.id = cppc.id_projet;

-- Vue pour les info db
DROP VIEW conf_projets.projet_database_vw;
CREATE OR REPLACE VIEW conf_projets.projet_database_vw AS
SELECT
    cpp.projet,
    cpps.selecteur,
    cpsd.tbl_name,
    cpsd.tbl_order,
    cpsd.is_partitionned,
    cpsd.partition_period,
    cpsd.load_strategy
FROM conf_projets.projet cpp
INNER JOIN conf_projets.selecteur_database cpsd ON cpp.id = cpsd.id_projet
INNER JOIN conf_projets.projet_selecteur cpps ON cpps.id = cpsd.id_selecteur
    AND cpps.id_projet = cpsd.id_projet
ORDER BY cpsd.tbl_order;


-- Vue pour selecteur_s3_db_info
DROP VIEW conf_projets.selecteur_s3_db_vw;
CREATE OR REPLACE VIEW conf_projets.selecteur_s3_db_vw AS
SELECT
    cpp.projet,
    cpps.selecteur,
    cpss3.filename,
    COALESCE(cpss3.key, cpps3.key) as s3_key,
    cpps3.bucket,
    cpps3.key as projet_s3_key,
    cpps3.key_tmp as projet_s3_key_tmp,
    CONCAT(COALESCE(cpss3.key, cpps3.key), '/', cpss3.filename) as filepath_s3,
    CONCAT(cpps3.key_tmp, '/', cpss3.filename) as filepath_tmp_s3,
    cpsd.tbl_name,
    cpsd.tbl_order,
    cpsd.is_partitionned,
    cpsd.partition_period,
    cpsd.load_strategy
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_selecteur cpps
  ON cpp.id = cpps.id_projet
INNER JOIN conf_projets.projet_s3 cpps3
  ON cpp.id = cpps3.id_projet
INNER JOIN conf_projets.selecteur_s3 cpss3
  ON cpps.id = cpss3.id_selecteur
    AND cpps.id_projet = cpss3.id_projet
INNER JOIN conf_projets.selecteur_database cpsd
  ON cpps.id = cpsd.id_selecteur
    AND cpps.id_projet = cpsd.id_projet
;
