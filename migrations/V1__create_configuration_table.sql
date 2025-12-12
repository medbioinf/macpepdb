CREATE TABLE system (
    id TEXT PRIMARY KEY,
    value JSON NOT NULL
);

SELECT create_reference_table('system');