CREATE TABLE IF NOT EXISTS staging.county_infirmary (
    id          bigserial PRIMARY KEY,
    row_data    jsonb       NOT NULL,
    reason      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);