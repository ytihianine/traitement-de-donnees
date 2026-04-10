-- Create
CREATE SCHEMA IF NOT EXISTS conf_projets;


DROP TABLE conf_projets."ref_direction" CASCADE;
CREATE TABLE conf_projets."ref_direction" (
  "id" serial,
  "id_direction" int,
  "direction" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_direction", "import_timestamp")
);

DROP TABLE conf_projets."ref_service" CASCADE;
CREATE TABLE conf_projets."ref_service" (
  "id" serial,
  "id_service" int,
  "id_direction" int,
  "service" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_service", "import_timestamp")
);


DROP TABLE conf_projets."projet" CASCADE;
CREATE TABLE conf_projets."projet" (
  "id" serial,
  "id_projet" int,
  "id_direction" int,
  "id_service" int,
  "projet" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "import_timestamp")
);


DROP TABLE conf_projets."projet_documentation" CASCADE;
CREATE TABLE conf_projets."projet_documentation" (
  "id" serial,
  "id_projet" int,
  "type_documentation" text,
  "lien" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "type_documentation", "import_timestamp")
);

DROP TABLE conf_projets."projet_s3" CASCADE;
CREATE TABLE conf_projets."projet_s3" (
  "id" serial,
  "id_projet" int,
  "bucket" text,
  "key" text,
  "key_tmp" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "import_timestamp")
);


DROP TABLE conf_projets."projet_contact" CASCADE;
CREATE TABLE conf_projets."projet_contact" (
  "id" serial,
  "id_contact" int,
  "id_projet" int,
  "contact_mail" text,
  "is_mail_generic" bool,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "id_contact", "import_timestamp")
);

DROP TABLE conf_projets."projet_selecteur" CASCADE;
CREATE TABLE conf_projets."projet_selecteur" (
  "id" serial,
  "id_selecteur" int,
  "id_projet" int,
  "type_selecteur" text,
  "selecteur" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
);

DROP TABLE conf_projets."selecteur_source" CASCADE;
CREATE TABLE conf_projets."selecteur_source" (
  "id" serial,
  "id_projet" int,
  "id_selecteur" int,
  "type_source" text,
  "id_source" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
);


DROP TABLE conf_projets."selecteur_s3" CASCADE;
CREATE TABLE conf_projets."selecteur_s3" (
  "id" serial,
  "id_projet" int,
  "id_selecteur" int,
  "key" text,
  "filename" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
);


DROP TABLE conf_projets."selecteur_database" CASCADE;
CREATE TABLE conf_projets."selecteur_database" (
  "id" serial,
  "id_projet" int,
  "id_selecteur" int,
  "tbl_name" text,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "id_selecteur", "import_timestamp")
);


DROP TABLE conf_projets."selecteur_column_mapping" CASCADE;
CREATE TABLE conf_projets."selecteur_column_mapping" (
  "id" serial,
  "id_col_mapping" int,
  "id_projet" int,
  "id_selecteur" int,
  "colname_source" text,
  "colname_dest" text,
  "to_keep" bool,
  "date_archivage" date,
  "import_timestamp" TIMESTAMP NOT NULL,
  "import_date" DATE NOT NULL,
  "snapshot_id" TEXT,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "id_selecteur", "id_col_mapping", "import_timestamp")
);


DROP TABLE conf_projets."projet_snapshot";
CREATE TABLE conf_projets."projet_snapshot"(
  "id" bigserial,
  "id_projet" int NOT NULL,
  "snapshot_id" text NOT NULL,
	"creation_timestamp" timestamp NOT NULL,
  PRIMARY KEY ("id"),
  UNIQUE ("id_projet", "snapshot_id")
)


-- Vue pour get_projet_s3_info()
DROP VIEW conf_projets.projet_s3_vw;
CREATE OR REPLACE VIEW conf_projets.projet_s3_vw AS
SELECT
    cpp.projet,
    cpps.bucket,
    cpps.key,
    cpps.key_tmp,
    cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cpp.import_timestamp ASC
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_s3 cpps ON cpp.id_projet = cpps.id_projet
  AND cpp.import_timestamp = cpps.import_timestamp;


-- Vue pour column_mapping_dataframe()
DROP VIEW conf_projets.cols_mapping_vw;
CREATE OR REPLACE VIEW conf_projets.cols_mapping_vw AS
SELECT
    cpp.projet,
    cpps.selecteur,
    scm.colname_source,
    scm.colname_dest,
    cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cpp.import_timestamp ASC
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_selecteur cpps ON cpp.id_projet = cpps.id_projet
  AND cpp.import_timestamp = cpps.import_timestamp
INNER JOIN conf_projets.selecteur_column_mapping scm
  ON cpps.id_selecteur = scm.id_selecteur
    AND cpps.id_projet = scm.id_projet
    AND cpp.import_timestamp = cpps.import_timestamp
WHERE scm.to_keep = true;


-- Vue pour get_list_documentation()
DROP VIEW conf_projets.projet_documentation_vw;
CREATE OR REPLACE VIEW conf_projets.projet_documentation_vw AS
SELECT
    cpp.projet,
    cppd.type_documentation,
    cppd.lien,
  cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cppd.import_timestamp ASC
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_documentation cppd ON cpp.id_projet = cppd.id_projet
  AND cpp.import_timestamp = cppd.import_timestamp;

-- Vue pour get_list_contact()
DROP VIEW conf_projets.projet_contact_vw;
create or replace
view conf_projets.projet_contact_vw as
select
	cpp.projet,
	cppc.contact_mail,
	cppc.is_mail_generic,
	cpp.import_timestamp,
	dense_rank() over (
order by
	cpp.import_timestamp asc
  ) as rang
from
	conf_projets.projet cpp
inner join conf_projets.projet_contact cppc on
	cpp.id_projet = cppc.id_projet
	and cpp.import_timestamp = cppc.import_timestamp;


-- Vue pour _get_selecteur_storage_info()
DROP VIEW conf_projets.selecteur_s3_db_vw;
CREATE OR REPLACE VIEW conf_projets.selecteur_s3_db_vw AS
SELECT
    cpp.projet,
    cpps.type_selecteur,
    cpps.selecteur,
    cpss.type_source,
    cpss.id_source,
    cpss3.filename,
    COALESCE(cpss3.key, cpps3.key) as s3_key,
    cpps3.bucket,
    cpps3.key as projet_s3_key,
    cpps3.key_tmp as projet_s3_key_tmp,
    CONCAT(COALESCE(cpss3.key, cpps3.key), '/', cpss3.filename) as filepath_s3,
    CONCAT(cpps3.key_tmp, '/', cpss3.filename) as filepath_tmp_s3,
    cpsd.tbl_name,
    cpp.import_timestamp,
    DENSE_RANK() OVER (
      ORDER BY cpp.import_timestamp ASC
  ) as rang
FROM conf_projets.projet cpp
INNER JOIN conf_projets.projet_selecteur cpps
  ON cpp.id_projet = cpps.id_projet
  AND cpp.import_timestamp = cpps.import_timestamp
LEFT JOIN conf_projets.selecteur_source cpss
  ON cpps.id_selecteur = cpss.id_selecteur
    AND cpps.id_projet = cpss.id_projet
  AND cpp.import_timestamp = cpss.import_timestamp
INNER JOIN conf_projets.projet_s3 cpps3
  ON cpp.id_projet = cpps3.id_projet
  AND cpp.import_timestamp = cpps3.import_timestamp
LEFT JOIN conf_projets.selecteur_s3 cpss3
  ON cpps.id_selecteur = cpss3.id_selecteur
    AND cpps.id_projet = cpss3.id_projet
  AND cpp.import_timestamp = cpss3.import_timestamp
LEFT JOIN conf_projets.selecteur_database cpsd
  ON cpps.id_selecteur = cpsd.id_selecteur
    AND cpps.id_projet = cpsd.id_projet
  AND cpp.import_timestamp = cpsd.import_timestamp
;
