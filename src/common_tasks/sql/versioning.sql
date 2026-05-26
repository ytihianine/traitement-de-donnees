CREATE SCHEMA IF NOT EXISTS versioning;

CREATE TABLE IF NOT EXISTS versioning.snapshot (
    id_row GENERATED ALWAYS AS IDENTITY,
    id_projet INTEGER NOT NULL,
    snapshot_id TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    is_dag_completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id_row)
);