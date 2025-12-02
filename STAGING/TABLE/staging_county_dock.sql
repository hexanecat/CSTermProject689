CREATE TABLE dock.county (
    fips_code text,
    state_code text,
    county_name text,
    attribute text,
    value text,
    insert_timestamp timestamp without time zone NOT NULL DEFAULT now(),
    processed_flag integer NOT NULL DEFAULT 0,
    processed_date timestamp without time zone
);

