create or replace function dbo.load_state_dim()
returns void
language plpgsql
as $$
declare
    v_rows_inserted integer;
begin
    insert into dbo.state_dim (state_code, state_name, census_region, current_flag, population)
    select distinct 
        c.state_code,
        m.state_name,
        m.census_region,
        'Y' as current_flag,
		population
    from staging.county c
    inner join map.state_code m 
        on m.state_code = c.state_code
    where not exists (
						select 1 
						from dbo.state_dim s2
						where s2.state_code = c.state_code
						) ;

    get diagnostics v_rows_inserted = row_count;

    perform etl.log_etl_event(
        'load_state_dim',
        'dbo.state_dim',
        'I',
        v_rows_inserted,
        'success',
        'Inserted new states from county_dim with mapping data',
        null
    );
end;
$$;