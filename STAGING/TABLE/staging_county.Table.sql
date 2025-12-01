CREATE TABLE staging.county
(
    fips_code        TEXT,
    state_code       TEXT,
    county_name      TEXT,
    attribute        TEXT,
    value            TEXT,
    insert_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_flag   INTEGER   NOT NULL DEFAULT 0,
    processed_date   TIMESTAMP
);