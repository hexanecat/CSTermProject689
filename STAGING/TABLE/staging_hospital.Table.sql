CREATE TABLE staging.hospital_dim
(
    facility_id                     TEXT,
    facility_name                   TEXT,
    address                         TEXT,
    city_town                       TEXT,
    state_code                      TEXT,
    zip_code                        TEXT,
    county_parish                   TEXT,
    telephone_number                TEXT,
    hospital_type                   TEXT,
    hospital_ownership              TEXT,
    emergency_services              TEXT,
    birthing_friendly_designation   TEXT,
    hospital_overall_rating         TEXT,
    -- derived fields (still TEXT or INT depending on your cleaning logic)
    emergency_flag                  INTEGER,
    birthing_friendly_flag          INTEGER,
    ownership_group                 TEXT,
    insert_timestamp                TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_flag                  INTEGER   NOT NULL DEFAULT 0,
    processed_date                  TIMESTAMP, 
	record_hash text
);