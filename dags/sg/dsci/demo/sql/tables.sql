DROP SCHEMA IF EXISTS "demo" CASCADE;
CREATE SCHEMA IF NOT EXISTS "demo";

/*
     Referenciels
*/
CREATE TABLE demo."ref_direction"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "direction" TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE demo."ref_type_demande"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "type_demande" TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
    Activité
*/

CREATE TABLE demo."demande"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_direction" int,
    "id_type_demande" int,
    "id_agent" int,
    "objet" text,
    "statut" text,
    "description" text,
    "niveau_de_complexite" text,
    "charge_estimee" numeric,
    "charge_consommee" numeric,
    "ecart_de_charge" numeric,
    "mail_contact" text,
    "date_creation" date,
    "date_modification" date,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE demo."equipe"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "agent" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);
