create or replace function dbo.load_hospital_dim_from_staging ()
returns void
language plpgsql
as $$

declare
    v_rows_processed integer := 0;
begin
	 update dbo.hospital_dim d
	 set
        --start by moving current values to prior year
		--since we should be pretending that the next time we load data with updated data it should be the current year
        emergency_flag_2024           = d.emergency_flag_2025,
        birthing_friendly_flag_2024   = d.birthing_friendly_flag_2025,
        overall_rating_2024           = d.overall_rating_2025,

        --now lets set the current year values from staging
        emergency_flag_2025           = case
                                            when s.emergency_flag = 1 then true
                                            when s.emergency_flag = 0 then false
                                            else null
                                        end,
        birthing_friendly_flag_2025   = case
                                            when s.birthing_friendly_flag = 1 then true
                                            when s.birthing_friendly_flag = 0 then false
                                            else null
                                        end,
        overall_rating_2025           = case
                                            when s.hospital_overall_rating ~ '^[0-9]+$'
                                                 then s.hospital_overall_rating::int
                                            else null
                                        end,
        --and this should include the new hash
        record_hash                   = s.record_hash
    from staging.hospital s
    where s.processed_flag = 1
      and d.facility_id   = s.facility_id
      --and this is the key piece here, this highlights when an existing record has undergone a change
      and d.record_hash  <> s.record_hash;  
    get diagnostics v_rows_processed = row_count;

    --log this event to our log table accordingly 
    perform etl.log_etl_event(
        'update hospital dim (SCD3)',
        'dbo.hospital_dim',
        'U',
        v_rows_processed,
        'success',
        'Completed hospital SCD Type 3 updates',
        null
    );

	--now this code here is going to handle inserts of brand new facilities (that have no facility id in the DIM table)
    insert into dbo.hospital_dim (
          facility_id
        , hospital_name
        , address
        , city
        , state_code
        , zip_code
        , county_name
        --current year scd type 3 values
        , emergency_flag_2025
        , birthing_friendly_flag_2025
        , overall_rating_2025
        --scd type 3 prior year values
        , emergency_flag_2024
        , birthing_friendly_flag_2024
        , overall_rating_2024
		, record_hash
    )
    select
          s.facility_id
        , s.facility_name
        , s.address
        , s.city_town
        , s.state_code::char(2)
        , s.zip_code
        , s.county_parish
		--SCD type 3 values
        , case 
              when s.emergency_flag = 1 then true
              when s.emergency_flag = 0 then false
              else null
          end                                         as emergency_flag_2025

        , case 
              when s.birthing_friendly_flag = 1 then true
              when s.birthing_friendly_flag = 0 then false
              else null
          end                                         as birthing_friendly_flag_2025

        , case 
              when s.hospital_overall_rating ~ '^[0-9]+$'
                   then s.hospital_overall_rating::smallint
              else null
          end                                         as overall_rating_2025
		  --make prior year null on initial load for now 
        , null                                        as emergency_flag_2024
        , null                                        as birthing_friendly_flag_2024
        , null                                        as overall_rating_2024
		, s.record_hash
    from staging.hospital s
    where s.processed_flag = 1
	and not exists (
      select 1
      from dbo.hospital_dim d
      where d.facility_id  = s.facility_id
        and d.record_hash  = s.record_hash --and make sure that if its the same facility id 
  );
      -- get row count of rows scrubbed
    get diagnostics v_rows_processed = row_count;
	
	    -- log success
    perform etl.log_etl_event(
        'insert into hospital dim',
        'dbo.hospital_dim',
        'I',
        v_rows_processed,
        'success',
        'Completed hospital insert',
        null
    );

end;
$$;