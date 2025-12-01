create or replace function dbo.load_hospital_ownership_dim()
returns void
language plpgsql
as $$
declare
    v_rows_inserted integer := 0;
begin
    -- log start
    perform etl.log_etl_event(
        'load_hospital_ownership_dim',
        'dbo.hospital_ownership_dim',
        'i',
        0,
        'started',
        'started loading hospital_ownership_dim from staging.hospital.',
        null
    );
	
    insert into dbo.hospital_ownership_dim (
        hospital_ownership_group_unscrubbed,
        ownership_group
    )
    select distinct
          s.hospital_ownership as hospital_ownership_group_unscrubbed,
          m.ownership_group
    from staging.hospital s
    join map.hospital_ownership_map m
      on lower(s.hospital_ownership) = lower(m.match_text)
    where s.hospital_ownership is not null
      and not exists (
            select 1
            from dbo.hospital_ownership_dim d
            where lower(d.hospital_ownership_group_unscrubbed)
                      = lower(s.hospital_ownership)
              and d.ownership_group = m.ownership_group
      );

    get diagnostics v_rows_inserted = row_count;

    -- log finish
    perform etl.log_etl_event(
        'load_hospital_ownership_dim',
        'dbo.hospital_ownership_dim',
        'i',
        v_rows_inserted,
        'success',
        'completed loading hospital_ownership_dim.',
        null
    );
end;
$$;