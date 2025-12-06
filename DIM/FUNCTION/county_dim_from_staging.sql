create or REPLACE function dbo.load_county_dim_from_staging ()
returns void language plpgsql as $$
declare
	v_rows_closed   integer := 0;
    v_rows_inserted integer := 0;
begin
	--log the start of an event 
	  perform etl.log_etl_event(
        'load_county_dim_from_staging',
        'dbo.county_dim',
        'scd2',
        0,
        'started',
        'started county dim scd2 load from staging.county',
        null
    );
	
	--handle SCD type 2 first, we need to close out rows where the record hash has changed
	update dbo.county_dim d
	set scd2_end_date = current_date - 1,
		current_flag  = 'N'
	from staging.county s
	where s.processed_flag = 1
		and d.fips_code = s.fips_code 
		and d.current_flag = 'Y'
		and d.record_hash <> s.record_hash;
	
	get diagnostics v_rows_closed = row_count;

	--only log something if we actually closed anything out
	if v_rows_closed > 0 then
		perform etl.log_etl_event(
			'load_county_dim_from_staging',
			'dbo.county_dim',
			'u',
			v_rows_closed,
			'success',
			'closed out existing county_dim rows for scd2 (hash changed)',
			null
		);
	end if;
	
	--now we need to go ahead and handle inserting either completely new rows or existing fips_code with new record_hash
	--if record hash does not exist and neither does the corresponding fips code then go ahead and insert
	insert into dbo.county_dim (
		 fips_code
		,state_code
		,county_name
		,median_household_income
		,poverty_rate
		,poverty_rate_0_17
		,deep_poverty_rate
		,record_hash
		,scd2_start_date
		,scd2_end_date
		,current_flag
		)
	select
		  s.fips_code
		, s.state_code
		, s.county_name
        , s.median_household_income::numeric      as median_household_income
        , s.poverty_rate::numeric                 as poverty_rate
        , s.poverty_rate_0_17::numeric            as poverty_rate_0_17
        , s.deep_poverty_rate::numeric            as deep_poverty_rate
        , s.record_hash                           as record_hash
        , current_date                            as scd2_start_date
        , date '9999-12-31'                       as scd2_end_date
        , 'Y'                                     as current_flag
	from staging.county s
	where s.processed_flag = 1
	and not exists (
		select 1
		from dbo.county_dim d
		where d.fips_code    = s.fips_code
		  and d.current_flag = 'Y'
		  and d.record_hash  = s.record_hash
      );
	  
	  --log changes that we made for new records, only do it if we did something 
	get diagnostics v_rows_inserted = row_count;

    if v_rows_inserted > 0 then
        perform etl.log_etl_event(
            'load_county_dim_from_staging',
            'dbo.county_dim',
            'i',
            v_rows_inserted,
            'success',
            'inserted new county_dim rows for scd2 (new or changed)',
            null
        );
    end if;
	
	perform etl.log_etl_event(
        'load_county_dim_from_staging',
        'dbo.county_dim',
        'scd2',
        v_rows_closed + v_rows_inserted,
        'success',
        'completed county dim scd2 load from staging.county',
        null
    );
	
end;$$;