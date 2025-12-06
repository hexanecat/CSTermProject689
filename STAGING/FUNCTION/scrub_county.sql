create or replace function staging.scrub_county_staging ()
returns void
language plpgsql
as $$
declare
    v_rows_processed integer := 0;
    v_rows_bad       integer := 0;
    v_temp_count     integer := 0;
begin
    -- log the start of the scrub
    perform etl.log_etl_event(
        'scrub_county_staging',
        'staging.county',
        'scrub',
        0,
        'started',
        'started county staging scrub process.',
        null
    );

    -- move values with non numeric values into the infirmary table
    with moved_numeric_vals as (
        insert into staging.county_infirmary (row_data, reason)
        select
            to_jsonb(c.*) as row_data,
            'non-numeric value found in one or more numeric fields' as reason
        from staging.county c
        where c.processed_flag = 0
          and (
                -- check if any required numeric field is non-numeric OR null
                (c.median_household_income is null or not public.is_numeric(c.median_household_income::text))
             or (c.poverty_rate is null or not public.is_numeric(c.poverty_rate::text))
             or (c.poverty_rate_0_17 is null or not public.is_numeric(c.poverty_rate_0_17::text))
             or (c.deep_poverty_rate is null or not public.is_numeric(c.deep_poverty_rate::text))
          )
        returning 1
    )
    select count(*) into v_rows_bad
    from moved_numeric_vals;

    -- delete those bad rows from staging
    delete from staging.county c
    where c.processed_flag = 0
      and (
            (c.median_household_income is null or not public.is_numeric(c.median_household_income::text))
         or (c.poverty_rate is null or not public.is_numeric(c.poverty_rate::text))
         or (c.poverty_rate_0_17 is null or not public.is_numeric(c.poverty_rate_0_17::text))
         or (c.deep_poverty_rate is null or not public.is_numeric(c.deep_poverty_rate::text))
      );

    -- now if there are bad rows, like ones that are not in the state map, remove these we do not want them
    with moved_bad_states as (
        insert into staging.county_infirmary (row_data, reason)
        select
            to_jsonb(c.*) as row_data,
            'invalid state code' as reason
        from staging.county c
        where c.processed_flag = 0
        and not exists (
            select 1
            from map.state_code m
            where m.state_code = c.state_code
        )
        returning 1
    )
    select count(*) into v_temp_count  
    from moved_bad_states;
    
    v_rows_bad := v_rows_bad + v_temp_count; 
    
    -- delete those bad rows from staging
    delete from staging.county c
    where c.processed_flag = 0
    and not exists (
        select 1
        from map.state_code m
        where m.state_code = c.state_code
    );
    
    -- log if any bad rows were moved
    if v_rows_bad > 0 then
        perform etl.log_etl_event(
            'scrub_county_staging',
            'staging.county',
            'scrub',
            v_rows_bad,
            'warning',
            format(
                'moved %s rows to staging.county_infirmary',
                v_rows_bad
            ),
            null
        );
    end if;
    
    -- generate record hash for all valid rows
    update staging.county
    set record_hash = encode(
            digest(
                concat_ws(
                    '|',
                    fips_code,
                    case when public.is_numeric(median_household_income) 
                        then (median_household_income::numeric(12,2))::text 
                        else 'NULL' end,
                    case when public.is_numeric(poverty_rate) 
                        then (poverty_rate::numeric(6,4))::text 
                        else 'NULL' end,
                    case when public.is_numeric(poverty_rate_0_17) 
                        then (poverty_rate_0_17::numeric(6,4))::text 
                        else 'NULL' end,
                    case when public.is_numeric(deep_poverty_rate) 
                        then (deep_poverty_rate::numeric(6,4))::text 
                        else 'NULL' end
                ),
                'sha256'
            ),
            'hex'
        )
    where processed_flag = 0;

    -- mark remaining rows as processed
    update staging.county c
    set processed_flag = 1,
        processed_date = now()
    where c.processed_flag = 0;

    get diagnostics v_rows_processed = row_count;

    perform etl.log_etl_event(
        'scrub_county_staging',
        'staging.county',
        'scrub',
        v_rows_processed,
        'success',
        'completed county staging scrub.',
        null
    );

end;
$$;