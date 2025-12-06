create or replace function dbo.load_state_dim()
returns void
language plpgsql
as $$
declare
    v_rows_inserted integer := 0;
    v_rows_updated integer := 0;
begin
    --SCD 1 updates, overwrite changes like population
    update dbo.state_dim s
    set 
        state_name = m.state_name,
        census_region = m.census_region,
        population = src.population
    from (
        select distinct 
            c.state_code,
            m.state_name,
            m.census_region,
            c.population
        from staging.county c
        inner join map.state_code m on m.state_code = c.state_code
    ) src
    inner join map.state_code m on m.state_code = src.state_code
    where s.state_code = src.state_code
      and (s.population is distinct from src.population  -- only update if changed
           or s.state_name is distinct from src.state_name
           or s.census_region is distinct from src.census_region);
    
    get diagnostics v_rows_updated = row_count;
    
    -- Log updates if any
    if v_rows_updated > 0 then
        perform etl.log_etl_event(
            'load_state_dim',
            'dbo.state_dim',
            'U',
            v_rows_updated,
            'success',
            'Updated existing states (SCD Type 1 overwrite)',
            null
        );
    end if;

    --then just insert new states that do not exist yet
    insert into dbo.state_dim (state_code, state_name, census_region, current_flag, population)
    select distinct 
        c.state_code,
        m.state_name,
        m.census_region,
        'Y' as current_flag,
        c.population
    from staging.county c
    inner join map.state_code m 
        on m.state_code = c.state_code
    where not exists (
        select 1 
        from dbo.state_dim s2
        where s2.state_code = c.state_code
    );

    get diagnostics v_rows_inserted = row_count;

    -- Log inserts
    perform etl.log_etl_event(
        'load_state_dim',
        'dbo.state_dim',
        'I',
        v_rows_inserted,
        'success',
        'Inserted new states (SCD Type 1)',
        null
    );
end;
$$;