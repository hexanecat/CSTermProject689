<img width="1831" height="1022" alt="image" src="https://github.com/user-attachments/assets/c9231c1c-631e-41e9-a460-3a4cf7652bbf" />


This is the constellation design of my term project for CS689. The goal here is to ingest two different data sources
-income.csv (census csv data from across the united states)
-hospital_general_information.csv (hospital data from across the united states report by CMS)

The data flow life cycle looks something like this 



[Term Project Data Flow Diagram.pdf](https://github.com/user-attachments/files/23945989/Term.Project.Data.Flow.Diagram.pdf)





1) bulk insert into staging.hospital and staging.county 
    -the target attributes we are getting from the file are found in map.header_targetcolumn
    -this will extract the corresponding attributes and insert them accordingly into the specified staging tables
2) scrub process for staging.hospital and staging.county
     -standardize key values
     -handle null values
     -handle clean values for typed fields like zip codes
3)insert into the corresponding dim tables  
     -we need to pivot the county staging table since the table is rolled up different  
     -we need to insert into hospital_dim and map county_bands based on values  
     -we need to insert into hospital ownership dim based on values mapped from the corresponding map table  
     -we need to insert into hospital type dim based on values mapped from corresponding map data  
     -we need to insert into state_dim for any new/updated values that were coming in  
     -we need to show the type SCD2 for county_dim for when we get updated records existing counties  
     -we need to show the type SCD3 for hospital_dim for when we get new data, and we push the older data to prior record (2024) and new to current (2025)  
4)insert into corresponding fact tables  
     -hospital_access_fact  
       -has grain per hospital, per hospital type per county. This gives us a good look at the hospitals that we want to look at close while also giving us county attributes for further analysis  
   -state_ownership_access_fact  
       -has grain per state per ownership. I want to see these type of relationships to see what hospitals have what type of states per ownership  


**SCHEMA DESIGN
**

DIM
  -where the main tables will live that will be the ones the fact tables will leverage the most

ETL
  -basic logging table
  -basic logging sproc to have inside each of the sql functions for transactional purposes

MAP
  -map tables will serve as a place for matching unscrubbed values to normalized ones that the database needs
  -designed to reduce the need for any hard coding and if we need to map something new, insert a record into the table

FACT
  -these will be mostly from the DIM tables and these will also have the computed measures that we need per the grain of each fact

STAGING
  -basic transactional tables that will serve as the starting point for getting data into the database and allowing us to start performing transformations/changes via SQL



