CREATE TABLE staging.county
(
    fips_code        TEXT,
    state_code       TEXT,
    county_name      TEXT,
	median_household_income TEXT,
	poverty_rate TEXT,
	poverty_rate_0_17 TEXT,
	deep_poverty_rate TEXT,
    insert_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_flag   INTEGER   NOT NULL DEFAULT 0,
    processed_date   TIMESTAMP, 
	record_hash		 TEXT
);