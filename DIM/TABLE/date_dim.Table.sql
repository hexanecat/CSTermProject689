CREATE TABLE dbo.date_dim (
    date_dim_id    SERIAL PRIMARY KEY,
    full_date      DATE NOT NULL,
    year           INTEGER NOT NULL,
    quarter        INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    day            INTEGER NOT NULL
);