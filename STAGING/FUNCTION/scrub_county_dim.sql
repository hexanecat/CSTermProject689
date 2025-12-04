create or replace function staging.scrub_county_staging ()
returns void
language plpgsql
as $$
declare
    v_rows_processed integer := 0;
    v_rows_bad       integer := 0;
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

	--move values with non numeric values into the infirmary table
	--made a simple is numeric function to handle this
	--want to log the entire row data as semi strcutured within single cell
	--jack mentioned that was a good idea and i found some json func inside postgres that does it
    with moved as (
        insert into staging.county_infirmary (row_data, reason)
        select
            to_jsonb(c.*) as row_data,
            'non-numeric value found in one or more numeric fields' as reason
        from staging.county c
        where c.processed_flag = 0
          and (
                (c.median_household_income is not null
                 and not public.is_numeric(c.median_household_income::text))
             or (c.poverty_rate is not null
                 and not public.is_numeric(c.poverty_rate::text))
             or (c.poverty_rate_0_17 is not null
                 and not public.is_numeric(c.poverty_rate_0_17::text))
             or (c.deep_poverty_rate is not null
                 and not public.is_numeric(c.deep_poverty_rate::text))
          )
        returning 1
    )
	
    --i want to keep track of the bad rows that we have for the sake of loggingg
    select count(*) into v_rows_bad
    from moved;

    -- delete those bad rows from staging
    delete from staging.county c
    where c.processed_flag = 0
      and (
            (c.median_household_income is not null
             and not public.is_numeric(c.median_household_income::text))
         or (c.poverty_rate is not null
             and not public.is_numeric(c.poverty_rate::text))
         or (c.poverty_rate_0_17 is not null
             and not public.is_numeric(c.poverty_rate_0_17::text))
         or (c.deep_poverty_rate is not null
             and not public.is_numeric(c.deep_poverty_rate::text))
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
                'moved %s rows with non-numeric numeric fields to staging.county_infirmary',
                v_rows_bad
            ),
            null
        );
    end if;

	--generate record hash for all valid rows
    update staging.county
    set record_hash = encode(
            digest(
                concat_ws(
                    '|',
                    median_household_income,
                    poverty_rate,
                    poverty_rate_0_17,
                    deep_poverty_rate
                ),
                'sha256'
            ),
            'hex'
        )
    where processed_flag = 0;

	--mark remaining rows as processed
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
