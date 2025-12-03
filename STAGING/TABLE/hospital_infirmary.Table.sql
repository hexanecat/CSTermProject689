CREATE TABLE IF NOT EXISTS staging.hospital_infirmary (
    id          bigserial PRIMARY KEY,
    row_data    jsonb       NOT NULL,
    reason      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);