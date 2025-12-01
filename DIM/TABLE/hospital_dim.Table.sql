CREATE TABLE dbo.hospital_dim (
    hospital_dim_id             SERIAL PRIMARY KEY,
    facility_id                 VARCHAR(20)  NOT NULL,
    hospital_name               VARCHAR(255) NOT NULL,
    address                     VARCHAR(255),
    city                        VARCHAR(100),
    state_code                  CHAR(2),
    zip_code                    VARCHAR(10),
    county_name                 VARCHAR(100),
    hospital_type               VARCHAR(100),
    -- SCD Type 3: ownership group
    ownership_group_2025        VARCHAR(50),
    ownership_group_2024        VARCHAR(50),

    -- SCD Type 3: emergency flag
    emergency_flag_2025         BOOLEAN,
    emergency_flag_2024         BOOLEAN,

    -- SCD Type 3: birthing-friendly flag
    birthing_friendly_flag_2025 BOOLEAN,
    birthing_friendly_flag_2024 BOOLEAN,

    -- SCD Type 3: overall rating
    overall_rating_2025         INT,
    overall_rating_2024         INT,
	--record hash column that is computed based off attrributes
	record_hash text
);