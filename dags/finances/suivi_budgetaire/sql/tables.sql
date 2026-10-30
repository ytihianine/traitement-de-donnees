DROP SCHEMA IF EXISTS suivi_budgetaire CASCADE;
CREATE SCHEMA IF NOT EXISTS suivi_budgetaire;


/*
  Tables sources
*/
CREATE TABLE suivi_budgetaire."nomenclature" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "exercice" int,
  "programme_code" int,
  "brique_code" int,
  "brique_libelle" text,
  "activite_code" text,
  "activite_libelle" text,
  "domaine_fonctionnel" text,
  "libelle_df" text,
  "bop" text,
  "uo" text,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE suivi_budgetaire."programmation_annuelle" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "exercice" int,
  "programme_code" int,
  "bop" text,
  "type" text,
  "brique_code" int,
  "action" text,
  "code" text,
  "libelle" text,
  "execution_annee_precedente_ae" numeric,
  "execution_annee_precedente_cp" numeric,
  "repartition_ressource_ae" numeric,
  "repartition_ressource_cp" numeric,
  "execution_ae" numeric,
  "execution_cp" numeric,
  "depense_obligatoire_cp" numeric,
  "depense_ineluctable_ae" numeric,
  "depense_ineluctable_cp" numeric,
  "depense_ineluctable_dont_rest_a_payer_cp" numeric,
  "autres_depenses_ae" numeric,
  "autres_depenses_cp" numeric,
  "total_depenses_programmees_ae" numeric,
  "total_depenses_programmees_cp" numeric,
  "type_depense" text,
  "rdv_budgetaire" text,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE suivi_budgetaire."execution_mensuelle" (
  id_row bigint GENERATED ALWAYS AS IDENTITY,
  "exercice" int,
  "programme_code" int,
  "bop" text,
  "uo" text,
  "activite_code" text,
  "activite_libelle" text,
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
  "programme_code" int,
  "programme_libelle" text,
  "brique_code" int,
  "brique_libelle" text,
  "type_montant" text,
  "montant_lfi" numeric,
  import_timestamp TIMESTAMP NOT NULL,
  snapshot_id UUID NOT NULL,
  snapshot_id_parent UUID NULL,
  PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);
