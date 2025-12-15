-- Scripts SQL générés automatiquement depuis fichiers Parquet
-- Date de génération: 2025-12-01 10:13:48.782623
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS donnee_comptable;
-- Table générée depuis: zjdp.parquet
-- Nombre de lignes: 38605
DROP TABLE donnee_comptable."demande_paiement_journal_pieces_sp" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_journal_pieces_sp (
	annee_exercice int4 NULL,
	annee_exercice_piece_fi int4 NULL,
	num_piece_reference int8 NULL,
	societe text NULL,
	centre_financier text NULL,
	centre_cout text NULL,
	id_dp text NULL,
	cf_cc text NULL,
	id_dp_cf_cc text NULL,
	nb_poste int4 NULL,
	unique_multi text NULL,
	import_timestamp timestamp NULL,
	import_date text NULL,
	service_prescripteur text NULL,
	snapshot_id text NULL
);


-- Table générée depuis: zdep61.parquet
-- Nombre de lignes: 501
DROP TABLE donnee_comptable."demande_paiement_carte_achat" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_carte_achat (
	num_piece_dp_carte_achat int4 NULL,
	societe text NULL,
	annee_exercice int4 NULL,
	automatisation_wf_cpt text NULL,
	niveau_carte_achat text NULL,
	statut_dp_carte_achat text NULL,
	id_dp text NULL,
	import_timestamp timestamp NULL,
	import_date text NULL
);

-- Table générée depuis: infdep56.parquet
-- Nombre de lignes: 39458
DROP TABLE donnee_comptable."delai_global_paiement_sp" CASCADE;
CREATE TABLE donnee_comptable.delai_global_paiement_sp (
	societe text NULL,
	annee_exercice int4 NULL,
	type_piece text NULL,
	nature_sous_nature text NULL,
	centre_financier text NULL,
	centre_cout text NULL,
	service_executant text NULL,
	nb_dp_dgp int4 NULL,
	montant_dp_dgp float8 NULL,
	delai_global_paiement float8 NULL,
	cf_cc text NULL,
	mois int,
	mois_nom text NULL,
	mois_nombre_nom text null,
	id int4 NULL,
	import_timestamp timestamp NULL,
	import_date text NULL,
	service_prescripteur text NULL,
	snapshot_id text NULL
);

-- Table générée depuis: infbud57.parquet
-- Nombre de lignes: 17648
DROP TABLE donnee_comptable."demande_achat_sp" CASCADE;
CREATE TABLE donnee_comptable.demande_achat_sp (
	id_da int4 NOT NULL,
	date_creation_da timestamp NULL,
	annee_exercice int4 NULL,
	centre_financier text NULL,
	centre_cout text NULL,
	date_replication timestamp NULL,
	montant_cumule_postes_da_traites float8 NULL,
	delai_traitement_da float8 NULL,
	cf_cc text NULL,
	id int4 NULL,
	mois int4 NULL,
	mois_nom text NULL,
	mois_nombre_nom text NULL,
	delai_traitement_classe varchar(255) NULL,
	import_timestamp timestamp NULL,
	import_date text NULL,
	service_prescripteur text NULL,
	snapshot_id text NULL
);

-- Table générée depuis: infbud55.parquet
-- Nombre de lignes: 14776
DROP TABLE donnee_comptable."demande_paiement_flux" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_flux (
	num_dp_flux int4 NULL,
	annee_exercice int4 NULL,
	societe text NULL,
	dp_flux_3 text NULL,
	id_dp text NULL,
	id int4 NULL,
	import_timestamp timestamp NULL,
	import_date text NULL
);


-- Table générée depuis: zsfp_suivi.parquet
-- Nombre de lignes: 3186
DROP TABLE donnee_comptable."demande_paiement_sfp" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_sfp (
	num_piece_sfp int4 NULL,
	societe text NULL,
	annee_exercice int4 NULL,
	num_ej_sfp float8 NULL,
	type_flux_sfp text NULL,
	statut_sfp text NULL,
	automatisation_wf_cpt text NULL,
	id_dp text NULL,
	import_timestamp timestamp NULL,
	import_date text NULL
);



-- Table générée depuis: zlisteej.parquet
-- Nombre de lignes: 6794
DROP TABLE donnee_comptable."engagement_juridique_sp" CASCADE;
CREATE TABLE donnee_comptable.engagement_juridique_sp (
	id_ej int8 NULL,
	type_ej text NULL,
	orga text NULL,
	gac text NULL,
	montant_ej float8 NULL,
	date_creation_ej timestamp NULL,
	centre_financier text NULL,
	centre_cout text NULL,
	cf_cc text NULL,
	ej_cf_cc text NULL,
	annee_exercice int4 NULL,
	mois int4 null,
	mois_nom text null,
	mois_nombre_nom text NULL,
	nb_poste_ej int4 NULL,
	unique_multi text NULL,
	type_ej_nom text NULL,
	import_timestamp timestamp NULL,
	import_date text NULL,
	service_prescripteur text NULL,
	snapshot_id text NULL
);

-- Table construite
DROP TABLE donnee_comptable.demande_paiement_complet_sp CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_complet_sp (
	annee_exercice int,
	societe text,
	nature_sous_nature float,
	type_piece_dp text,
	centre_financier text,
	date_comptable date NULL,
	num_dp int8 NULL,
	montant_dp double precision,
	statut_piece text,
	id_dp text,
	mois int,
	mois_nom text,
	mois_nombre_nom text,
	nat_snat_nom text,
	nat_snat_groupe text,
	import_timestamp timestamp,
	import_date date,
	annee_exercice_piece_fi bigint,
	num_piece_reference bigint,
	centre_cout text,
	cf_cc text,
	id_dp_cf_cc text,
	nb_poste bigint,
	unique_multi text,
	num_piece_dp_carte_achat bigint,
	automatisation_wf_cpt text,
	niveau_carte_achat text,
	statut_dp_carte_achat text,
	num_dp_flux bigint,
	dp_flux_3 text,
	id bigint,
	num_piece_sfp bigint,
	num_ej_sfp bigint,
	type_flux_sfp text,
	flux_3_automatisation_compta text,
	statut_sfp text,
	snapshot_id text
);
