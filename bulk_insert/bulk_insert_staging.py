#this is the bulk insert into staging
#the goal for this is to put the csvs into objects and then strip out the data/attributes that we need for them from each and prep them
#for inserts into the staging tables
from sqlalchemy import create_engine, text
import pandas as pd

#read in each file
hospitalFile = "Hospital_General_Information_updated2.csv"
hospital_df = pd.read_csv(hospitalFile)

#income data
incomeFile = "Income.csv"
income_df = pd.read_csv(incomeFile, encoding="latin1", on_bad_lines="skip")


#
#do all the extracting of attributes here to prep for the staging insert
#we need to extractand rename columns to match to staging.hospital
#then we need to do the same for staging.county
#maybe make the map table something in the database and if we ever need to read from it we can just pull straight from there
#and have it all be dynamic
#
hospital_column_map = {
    "Facility ID": "facility_id",
    "Facility Name": "facility_name",
    "Address": "address",
    "City/Town": "city_town",
    "State": "state_code",
    "ZIP Code": "zip_code",
    "County/Parish": "county_parish",
    "Telephone Number": "telephone_number",
    "Hospital Type": "hospital_type",
    "Hospital Ownership": "hospital_ownership",
    "Emergency Services": "emergency_services",
    # adjust this if your maternal health column is named differently
    "Meets criteria for birthing friendly designation": "birthing_friendly_designation",
    "Hospital overall rating": "hospital_overall_rating",
}

#after this we need to rename the columns that we care about for the dataframe that we are doing an insert into
hospital_stage_df = hospital_df[list(hospital_column_map.keys())].rename(
    columns=hospital_column_map
)

#
#now do the bulk insert into staging
#

county_column_map = {
    "FIPS": "fips_code",
    "State": "state_code",
    "County": "county_name",
    "Attribute": "attribute",
    "Value": "value",
}

county_stage_df = income_df[list(county_column_map.keys())].rename(
    columns=county_column_map
)

#make engine 
username = "postgres"
password = "frankocean"
host = "localhost"
port = "5432"
database = "TermProject"

#build engine to be able to connect to the postgres database
engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
print("Engine URL:", engine.url)

#since these are staging tables, we will want to do truncate and reload on all of them 
with engine.begin() as conn:
    # clear out old data for a clean reload
    conn.execute(text("TRUNCATE TABLE staging.hospital_dim;"))
    conn.execute(text("TRUNCATE TABLE staging.county_dim;"))

    # insert into staging.hospital
    hospital_stage_df.to_sql(
        name="hospital_dim",
        schema="staging",
        con=conn,
        if_exists="append",
        index=False,
    )

    # insert into staging.county_dim
    county_stage_df.to_sql(
        name="county_dim",
        schema="staging",
        con=conn,
        if_exists="append",
        index=False,
    )

print("Staging loads complete:")
print(f"  staging.hospital rows:   {len(hospital_stage_df)}")
print(f"  staging.county_dim rows: {len(county_stage_df)}")

#after the staging comppletes we will want to trigger the stored procedures for scrubbing the data.
#rows inserted into staging now we want to trigger the scrub
#then after the scrub we want to trigger loading into the dims 
#then after that insert into the fact tables

# with engine.connect() as conn:
#     #we need to run our scrubs that just got the inserts from staging
#     conn.execute(text("select staging.scrub_county_staging();"))
#     conn.execute(text("select staging.scrub_hospital_staging();"))

#     #then based off the scrubbed staging data we need to loaded the dim tables
#     conn.execute(text("select dbo.load_county_dim_from_staging();"))
#     conn.execute(text("select dbo.load_hospital_dim_from_staging();"))

#     #then finally we need to load the access fact table and right now datedim just one cause i only have one val there
#     #and maybe i will make this different not sure yet
#     conn.execute(text("select dbo.load_hospital_access_fact(1);"))

#     conn.commit()