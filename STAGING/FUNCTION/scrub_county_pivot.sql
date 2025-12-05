create or replace function staging.pivot_county_staging()
returns void
language plpgsql
as $$
declare v_rows_processed integer := 0;
begin

    -- log the start of the scrub
    perform etl.log_etl_event(
        'pivot_county_staging',
        'staging.county',
        'scrub',
        0,
        'started',
        'started county staging pivot process.',
        null
    );



insert into staging.county (
		 fips_code
		,state_code
		,county_name
		,median_household_income
		,poverty_rate
		,poverty_rate_0_17
		,deep_poverty_rate
		)
	/*
		we need to pivot this data 
		easiest way to do this i found was through using case statements inside max functions 
		you could probably use min here and it would be the same function
	*/
	select
		 s.fips_code::char(5)
		,s.state_code::char(2)
		,s.county_name
		,MAX(case 
				when s.attribute = 'Median_HH_Inc_ACS'
					then NULLIF(s.value, '')::numeric(12,2)
				end) as median_household_income
		,MAX(case 
				when s.attribute = 'Poverty_Rate_ACS'
					then NULLIF(s.value, '')::numeric(6, 4)
				end) as poverty_rate
		,MAX(case 
				when s.attribute = 'Poverty_Rate_0_17_ACS'
					then NULLIF(s.value, '')::numeric(6, 4)
				end) as poverty_rate_0_17
		,MAX(case 
				when s.attribute = 'Deep_Pov_All'
					then NULLIF(s.value, '')::numeric(6, 4)
				end) as deep_poverty_rate
	from dock.county s
	--and for the pivot this is the key piece beacuse i want to group EVERYTHING together at the grain of FIPS code, state_code and county
	--I only want a single row for that grain
	group by s.fips_code
		   , s.state_code
		   , s.county_name
	order by s.fips_code; --i want to sort by fips code for more stability 
		   
	
 -- capture number of rows updated
    get diagnostics v_rows_processed = row_count;
	
	update dock.county
	set processed_flag = 1, 
		processed_date = NOW()
	where processed_flag = 0;  
	
    -- log success
    perform etl.log_etl_event(
        'pivot county data',
        'dock.county',
        'scrub',
        v_rows_processed,
        'success',
        'completed county staging scrub.',
        null
    );
	
end;
$$;