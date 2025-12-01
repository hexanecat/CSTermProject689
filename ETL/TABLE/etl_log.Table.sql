CREATE TABLE etl.etl_log (
    log_id          SERIAL PRIMARY KEY,
    process_name    VARCHAR(100) NOT NULL,    
    target_table    VARCHAR(150) NOT NULL,    
    action_type     VARCHAR(10) NOT NULL,      -- I, U, D, INF
    row_count       INTEGER,                   
    status          VARCHAR(20) NOT NULL,      
    message         TEXT,                      
    source_file     VARCHAR(500),              
    log_tms         TIMESTAMP NOT NULL DEFAULT now()    
);


--JSON data type put a key column 
--create attriutes and nesting 
--single table key value structure
--2 tables one defines what is the table 2 is the lookup values
--only so much you can do with wideness
--semistrcutured architecture 
--defintiely 



--what about logical primary keys?
--if the source data does not have the corresponding primary key, boot it 
--record hash, concat a pipe delimited string based on a set of values in a row_count
--compute the hash, and then on the insert into the warehouse if that pl/record hash does not exist 
--then dont update anything
--and if there are updates go ahead and update it 