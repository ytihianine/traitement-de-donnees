CREATE TABLE IF NOT EXISTS sircom.indicateurs_metiers (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    indicateurs TEXT,
    valeur DOUBLE PRECISION,
    unite TEXT,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.enquete_de_satisfaction (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    indicateurs TEXT,
    valeur DOUBLE PRECISION,
    unite TEXT,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.etudes (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    demandeurs TEXT,
    etudes INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.communique_presse (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    communiques_presse INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.creation_graphique (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    demandeurs TEXT,
    creation_graphique INTEGER,
    is_last_value BOOLEAN
);
CREATE TABLE IF NOT EXISTS sircom.notes_veilles (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    nombre_note INTEGER,
    nombre_signalements INTEGER,
    is_last_value BOOLEAN
);
CREATE TABLE IF NOT EXISTS sircom.projets_graphiques(
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    commanditaire TEXT,
    nombre_projets_graphique INTEGER,
    nombre_graphique_realise INTEGER,
    is_last_value BOOLEAN
);
CREATE TABLE IF NOT EXISTS sircom.recommandation_strat(
    id BIGSERIAL PRIMARY KEY,
    nombre_recommandation INTEGER,
    date DATE,
    is_last_value BOOLEAN

);
