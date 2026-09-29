DROP SCHEMA IF EXISTS "demo_traitement" CASCADE;
CREATE SCHEMA IF NOT EXISTS "demo_traitement";

/*
     Referenciels
*/
CREATE TABLE demo_traitement."ref_direction"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "direction" TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE demo_traitement."ref_intervention"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "typologie_d_intervention2" TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
     Activité
*/

CREATE TABLE demo_traitement."accompagnement"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_direction" int,
    "id_type_intervention" int,
    "id_assignation" int,
    "date_de_la_demande" date,
    "accompagnement" text,
    "statut" text,
    "niveau_de_complexite" text,
    "charge_estimee" numeric,
    "charge_consommee" numeric,
    "ecart_de_charge" numeric,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);
