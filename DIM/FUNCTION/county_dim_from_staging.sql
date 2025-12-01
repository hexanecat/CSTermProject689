create or REPLACE function dbo.load_county_dim_from_staging ()
returns void language plpgsql as $$

begin
	insert into dbo.county_dim (
		fips_code
		,state_code
		,county_name
		,median_household_income
		,poverty_rate
		,poverty_rate_0_17
		,deep_poverty_rate
		,scd2_start_date
		,scd2_end_date
		,current_flag
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
		,current_date as scd2_start_date
		,date '9999-12-31' as scd2_end_date
		,'Y' as current_flag
	from staging.county s
	where s.processed_flag = 1
	group by s.fips_code
		   , s.state_code
		   , s.county_name;
end;$$;