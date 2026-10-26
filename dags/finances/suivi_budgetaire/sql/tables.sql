DROP SCHEMA IF EXISTS suivi_budgetaire CASCADE;
CREATE SCHEMA IF NOT EXISTS suivi_budgetaire;


/*
    Référentiels commun
*/
CREATE TABLE suivi_budgetaire."nomenclature" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "exercice" int,
  "programme" int,
  "brique" int,
  "libelle_brique" text,
  "activite" text,
  "libelle_activite" text,
  "domaine_fonctionnel" text,
  "libelle_df" text,
  "bop" text,
  "uo" text,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE suivi_budgetaire."programmation_budgetaire" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,

  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE suivi_budgetaire."execution_mensuelle" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "exercice" int,
  "programme" int,
  "bop" text,
  "uo" text,
  "numero_activite" text,
  "libelle_activite" text,
  "titre" text,
  "domaine_fonctionnel" text,
  "fonds" text,
  "numero_mois" int,
  "mois" text,
  "date_execution" date,
  "type_montant" text,
  "montant_execution" numeric,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE suivi_budgetaire."lfi_brique" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "exercice" int,
  "programme" int,
  "libelle_programme" text,
  "brique" int,
  "libelle_brique" text,
  "type_montant" text,
  "montant_lfi" numeric,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);
