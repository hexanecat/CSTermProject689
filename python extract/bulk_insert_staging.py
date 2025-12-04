from sqlalchemy import create_engine, text
import pandas as pd
import os

#basic logging function to serve as a trace for file origin
def log_results(engine, processName, rowCnt, targetTable, fileName, action_type, message, status):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl.etl_log (
                        process_name,
                        target_table,
                        action_type,
                        rows_processed,
                        status,
                        message,
                        source_file
                    ) VALUES (
                        :process_name,
                        :target_table,
                        :action_type,
                        :rows_processed,
                        :status,
                        :message,
                        :source_file
                    )
                """),
                {
                    "process_name": processName,
                    "target_table": targetTable,
                    "action_type": action_type,
                    "rows_processed": rowCnt,
                    "status": status,
                    "message": message,
                    "source_file": fileName
                }
            )
    #if logger fails throw hard error
    except Exception as e:
        print ("failed to log ETL result: ", e)
        raise

#again we want to do column mappings based on the dynamic value that we set per table so that is why
#we are not hardcoing anything here everything is going to be in the columns that we are fetching here
def get_column_map(engine, dataset_name: str) -> dict:
    query = """
        SELECT source_column, target_column
        FROM map.header_targetcolumn
        WHERE target_table = %(dataset_name)s
          AND is_active = TRUE
    """
    df_map = pd.read_sql(query, engine, params={"dataset_name": dataset_name})
    return dict(zip(df_map["source_column"], df_map["target_column"]))

#simple truncate table function for truncating all of the staging tables that raw data gets ingested into
#this should be done at the start of every pipeline that needs to be triggered
def truncate_staging_tables(engine):
    try:
        with engine.begin() as conn:
            # truncate tables dynamically via truncate sproc
            conn.execute(text("SELECT truncate_staging_tables();"))
    except Exception as e:
        print(f"Error: {e}")

#function to insert data into staging
def insert_data_into_staging(engine, df, targetTable, fileName, processName,targetSchema):
    processName = "insert " + fileName + " into " + targetTable
    rowCnt = 0
    try:
        with engine.begin() as conn:
            df.to_sql(
                name= targetTable,
                schema=targetSchema,
                con=conn,
                if_exists="append",
                index=False,
            )
            rowCnt = len(df)
            #this will be a success log
            message = 'rows inserted into staging successfully'
            log_results(engine,processName,rowCnt, targetTable, fileName, 'I',message, 'success')

    #if there is an error log then log it here
    except Exception as e:
        error_msg = str(e)
        log_results(engine,processName,rowCnt, targetTable, fileName, 'E', error_msg, 'error')



def main():
    #read in each file both the hospital data and the income data from the csvs we have generated
    hospitalFile = "mockdata/Hospital_General_Information_updated2.csv"
    hospitalFileName = os.path.basename(hospitalFile)
    hospital_df = pd.read_csv(hospitalFile)

    countyFile = "mockdata/Income.csv"
    countyFileName = os.path.basename(countyFile)
    county_df = pd.read_csv(countyFile, encoding="latin1", on_bad_lines="skip")


    #make engine to be able to connect to postgres db
    #make svcAccount so i can query the db using my other acct
    username = "svcAccount"
    password = "ibettergetanA"
    host = "localhost"
    port = "5432"
    database = "TermProject"

    #build engine to be able to connect to the postgres database
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
    print("Engine URL:", engine.url)

    #dynamically get column values here
    hospital_column_map = get_column_map(engine, "hospital")

    #we only want to keep/rename the columns that we need inserted into the dataframe for both
    hospital_stage_df = hospital_df[list(hospital_column_map.keys())] \
        .rename(columns=hospital_column_map)

    county_column_map = get_column_map(engine, "county")
    county_stage_df = county_df[list(county_column_map.keys())] \
        .rename(columns=county_column_map)

    #since these are staging tables, we will want to do truncate and reload on all of them 
    try:    
        truncate_staging_tables(engine)
    except Exception as e:
        #cast the error message as a string so we can put it in the db
        error_msg = str(e)

    #once tables are truncated continue on to the next step
    #start inserting data into the staging table
    #in my eyes if data does not even get into staging that is something that we need to work with a client on
    try:    
        insert_data_into_staging(engine, hospital_stage_df, 'hospital', hospitalFileName, 'insert into staging','staging')
        #remember i have to insert into preprocessing table first so that is why we are aiming to do a dock instead
	#dataflow here will be dock and then it will be staging. python will handle the doc
	insert_data_into_staging(engine, county_stage_df, 'county', countyFileName, 'insert into ','dock')
    except Exception as e:
        #cast the error message as
        error_msg = str(e)

    #when we are done using the engine at the end just dispose and get rid of it
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
