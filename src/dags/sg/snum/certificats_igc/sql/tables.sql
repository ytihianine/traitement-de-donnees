DROP SCHEMA certificat_igc CASCADE;
CREATE SCHEMA certificat_igc;

CREATE TABLE certificat_igc.certificat (
    id SERIAL,
    id_certificat BIGINT,
    subjectid TEXT,
    contact TEXT,
    email  TEXT,
    date_debut_validite DATE,
    date_fin_validite DATE,
    profile TEXT,
    ac TEXT,
    type_offre TEXT,
    supports TEXT,
    version TEXT,
    version_serveur TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT,
    PRIMARY KEY(id, import_date)
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
    snapshot_id TEXT,
    PRIMARY KEY(id, import_date)
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.agent (
    id SERIAL,
    nom_prenom TEXT,
    agent_direction TEXT,
    agent_mail TEXT,
    agent_groupe_gestionnaire TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT,
    PRIMARY KEY(id, import_date)
) PARTITION BY RANGE (import_date);
