CREATE TABLE IF NOT EXISTS config (
    conf_key VARCHAR(256),
    value JSONB NOT NULL,
    PRIMARY KEY (conf_key)
);