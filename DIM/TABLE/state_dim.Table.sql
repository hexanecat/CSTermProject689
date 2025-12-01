create table dbo.state_dim (
    state_dim_id      serial primary key,
    state_code        char(2) not null,     -- 'CA', 'NY', etc.
    state_name        varchar(100),        -- 'California'
    census_region     varchar(50),         -- 'West', 'Northeast', etc. (optional)
    current_flag      char(1) default 'Y'
);