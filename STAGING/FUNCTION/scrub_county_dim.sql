create or replace function staging.scrub_county_staging ()
returns void
language plpgsql
as $$
declare v_rows_processed integer := 0;
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

    -- scrub county staging rows
    update staging.county
    set  fips_code      = lpad(btrim(fips_code), 5, '0'),
         state_code     = upper(btrim(state_code)),
         county_name    = upper(county_name),
         attribute      = btrim(attribute),
         value          = btrim(value),
         processed_flag = 1,
         processed_date = now()
    where processed_flag = 0;

    -- capture number of rows updated
    get diagnostics v_rows_processed = row_count;

    -- log success
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
