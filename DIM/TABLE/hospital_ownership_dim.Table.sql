create table dbo.hospital_ownership_dim (
    hospital_ownership_dim_id serial primary key,
    hospital_ownership_group_unscrubbed varchar(250),
    ownership_group           varchar(50) not null
  );

