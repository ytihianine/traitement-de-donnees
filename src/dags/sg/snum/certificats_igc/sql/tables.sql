DROP SCHEMA certificat_igc CASCADE;
CREATE SCHEMA certificat_igc;

CREATE TABLE certificat_igc.certificat (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    id_certificat BIGINT,
    subjectid TEXT,
    contact TEXT,
    email  TEXT,
    date_debut_validite DATE,
    date_fin_validite DATE,
    profile TEXT,
    status  TEXT,
    ac TEXT,
    type_offre TEXT,
    supports TEXT,
    etat TEXT,
    version TEXT,
    version_serveur TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT,
    PRIMARY KEY(id, import_date)
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.mandataire (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
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
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    nom_prenom TEXT,
    agent_direction TEXT,
    agent_mail TEXT,
    agent_groupe_gestionnaire TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT,
    PRIMARY KEY(id, import_date)
) PARTITION BY RANGE (import_date);
