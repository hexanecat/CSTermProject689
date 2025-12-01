CREATE TABLE map.hospital_type_map (
    match_text TEXT NOT NULL,               -- e.g. 'non-profit', 'for-profit', 'government'
    hospital_type TEXT NOT NULL           -- e.g. 'NON_PROFIT', 'FOR_PROFIT', 'GOVERNMENT'
);