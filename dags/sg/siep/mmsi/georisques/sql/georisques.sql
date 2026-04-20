DROP TABLE IF EXISTS siep.bien_georisque;
CREATE TABLE IF NOT EXISTS siep.bien_georisque (
    code_bat_ter BIGINT NOT NULL,
    risque_categorie TEXT,
    risque_libelle TEXT,
    risque_present BOOLEAN,
    statut TEXT,
    statut_code INT,
    raison TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    snapshot_id TEXT,
    PRIMARY KEY (snapshot_id, import_timestamp, code_bat_ter, risque_categorie, risque_libelle)
) PARTITION BY RANGE (import_timestamp) ;
