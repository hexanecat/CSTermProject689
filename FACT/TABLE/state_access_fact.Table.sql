create table dbo.state_ownership_access_fact (
      date_dim_id                   integer not null
    , state_dim_id                  integer not null
    , hospital_ownership_dim_id     integer not null
    , hospital_count                integer not null
    , hospitals_per_100k            numeric(10,2)
    , safe_birthing_places_per_100k numeric(10,2)
    , emergency_hospital_access_per_100k numeric(10,2)
	, average_hospital_rating numeric (10,2)
    , constraint pk_state_ownership_access_fact primary key (
          date_dim_id
        , state_dim_id
        , hospital_ownership_dim_id
      )
    , constraint fk_soa_date foreign key (date_dim_id)
        references dbo.date_dim (date_dim_id)
    , constraint fk_soa_state foreign key (state_dim_id)
        references dbo.state_dim (state_dim_id)
    , constraint fk_soa_ownership foreign key (hospital_ownership_dim_id)
        references dbo.hospital_ownership_dim (hospital_ownership_dim_id)
);
