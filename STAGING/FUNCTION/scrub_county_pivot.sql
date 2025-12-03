create or replace function staging.pivot_county_staging ()
returns void
language plpgsql
as $$
declare v_rows_processed integer := 0;
begin


insert into staging.county (
		 fips_code
		,state_code
		,county_name
		,median_household_income
		,poverty_rate
		,poverty_rate_0_17
		,deep_poverty_rate
		)
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
	group by s.fips_code
		   , s.state_code
		   , s.county_name
	order by s.fips_code; --i want to sort by fips code for more stability 
		   

 -- capture number of rows updated
    get diagnostics v_rows_processed = row_count;

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