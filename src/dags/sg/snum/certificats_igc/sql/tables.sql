DROP SCHEMA certificat_igc CASCADE;
CREATE SCHEMA certificat_igc;

CREATE TABLE certificat_igc.aip (
    id SERIAL,
    aip_mail TEXT,
    aip_balf_mail TEXT,
    aip_direction_geree TEXT,
    structure TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.certificat (
    id SERIAL,
    dn TEXT,
    subjectid TEXT,
    contact TEXT,
    email  TEXT,
    date_debut_validite DATE,
    date_fin_validite DATE,
    profile TEXT,
    status  TEXT,
    date_revocation DATE,
    certif_dir_profile TEXT,
    certif_dir_dn TEXT,
    certif_dir_subjectid TEXT,
    certif_dir_contact TEXT,
    certif_dir_mail TEXT,
    certificat_direction TEXT,
    ac TEXT,
    type_offre TEXT,
    supports TEXT,
    etat TEXT,
    version TEXT,
    version_serveur TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.historique_certificat (
    id SERIAL,
    agent_mail TEXT,
    cn TEXT,
    agent_structure TEXT,
    date_fin_validite DATE,
    date_debut_validite DATE,
    agent_direction TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.mandataire (
    id SERIAL,
    libelle TEXT,
    sigle TEXT,
    mail TEXT,
    structure TEXT,
    date DATE,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT
) PARTITION BY RANGE (import_date);
