CREATE TABLE dbo.county_dim (
    county_dim_id              SERIAL PRIMARY KEY,
    fips_code                  CHAR(5) NOT NULL,
    state_code                 CHAR(2) NOT NULL,
    county_name                VARCHAR(100) NOT NULL,
    median_household_income    NUMERIC(12,2),
    poverty_rate               NUMERIC(6,4),
    poverty_rate_0_17          NUMERIC(6,4),
    deep_poverty_rate          NUMERIC(6,4),
	poverty_band           	   VARCHAR(20),
	income_band                VARCHAR(20),
	deep_poverty_band          VARCHAR(30),
    record_hash                TEXT,
    -- SCD2 attributes
    scd2_start_date             DATE NOT NULL,
    scd2_end_date               DATE NOT NULL,
    current_flag               CHAR(1) NOT NULL CHECK (current_flag IN ('Y','N'))
);
