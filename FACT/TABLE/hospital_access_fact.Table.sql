create table dbo.hospital_access_fact (
	  date_dim_id integer not null
	, hospital_dim_id integer not null
	, county_dim_id integer not null
	, hospital_type_dim_id integer not null
	, poverty_band        VARCHAR(20)
	, income_band         VARCHAR(20)
	, deep_poverty_band   VARCHAR(30)
	, high_risk_area      INTEGER
	, risk_access_index numeric(6,2)
	, availability_score  NUMERIC(6,4)
	, quality_score  NUMERIC(6,4)
	, constraint pk_hospital_access_fact primary key (
		date_dim_id
		,hospital_dim_id
		)
	, constraint fk_haf_date foreign key (date_dim_id) references dbo.date_dim(date_dim_id)
	, constraint fk_haf_hospital foreign key (hospital_dim_id) references dbo.hospital_dim(hospital_dim_id)
	, constraint fk_haf_county foreign key (county_dim_id) references dbo.county_dim(county_dim_id)
	, constraint fk_haf_hospital_ownership foreign key (hospital_type_dim_id) references dbo.hospital_type_dim
	);