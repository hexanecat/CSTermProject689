create or replace function dbo.load_hospital_type_dim()
returns void
language plpgsql
as $$
declare
    v_rows_inserted integer := 0;
begin
    --log the start of this
    perform etl.log_etl_event(
        'load_hospital_type_dim',
        'dbo.hospital_type_dim',
        'i',
        0,
        'started',
        'started loading hospital_type_dim from staging.hospital.',
        null
    );

	--for scd type one just do a direct insert like this 
    insert into dbo.hospital_type_dim (hospital_type)
    select distinct
           s.hospital_type
    from staging.hospital s
    where s.hospital_type is not null
      and not exists (
          select 1
          from dbo.hospital_type_dim d
          where d.hospital_type = s.hospital_type
      );

    get diagnostics v_rows_inserted = row_count;

    -- log success
    perform etl.log_etl_event(
        'load_hospital_type_dim',
        'dbo.hospital_type_dim',
        'i',
        v_rows_inserted,
        'success',
        'completed loading hospital_type_dim from staging.hospital.',
        null
    );
end;
$$;
