CREATE TABLE map.header_targetcolumn(
    id              SERIAL PRIMARY KEY,
    target_table    VARCHAR(50) NOT NULL,     
    source_column   VARCHAR(255) NOT NULL,     
    target_column   VARCHAR(255) NOT NULL,    
    is_active       BOOLEAN NOT NULL DEFAULT TRUE
);