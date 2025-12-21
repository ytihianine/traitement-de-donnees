DROP TABLE IF EXISTS siep.ref_typologie CASCADE;
CREATE TABLE IF NOT EXISTS siep.ref_typologie (
    bati_non_bati TEXT,
    famille_de_bien_simplifiee TEXT,
    famille_de_bien TEXT,
    type_de_bien TEXT,
    usage_detaille_du_bien TEXT UNIQUE NOT NULL,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT,
    PRIMARY KEY (snapshot_id, code_bat_ter, usage_detaille_du_bien)
);
