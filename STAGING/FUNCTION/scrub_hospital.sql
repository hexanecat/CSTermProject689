create or replace function staging.scrub_hospital_staging ()
returns void
language plpgsql
as $$
declare
    v_rows_processed integer := 0;
    v_rows_bad       integer := 0;
    v_zip_bad_count  integer := 0;
    v_birthing_bad_count integer := 0;
begin
    -- log the start of the scrub
    perform etl.log_etl_event(
        'scrub_hospital_staging',
        'staging.hospital',
        'scrub',
        0,
        'started',
        'Started hospital scrub process.',
        null
    );

    -- ensure the zip is 5 chars and not anything more
    --WORK ON HANDLING BAD ZIP CODE VALUES 
 with bad_zip_rows as (
        select h.*,
               case 
                   when h.zip_code is null then 'zip code is null'
                   when length(btrim(h.zip_code)) != 5 then 'zip code must be exactly 5 characters'
                   when not h.zip_code ~ '^[0-9]{5}$' then 'zip code must contain only digits'
                   else 'invalid zip code format'
               end as reason
        from staging.hospital h
        where h.processed_flag = 0
          and (
               h.zip_code is null 
               or length(btrim(h.zip_code)) != 5
               or not h.zip_code ~ '^[0-9]{5}$'
          )
    ),
    moved_zip as (
        insert into staging.hospital_infirmary (row_data, reason)
        select to_jsonb(bz.*), bz.reason
        from bad_zip_rows bz
        returning *
    )
    select count(*) into v_zip_bad_count
    from moved_zip;
    
    -- delete invalid zip code rows from staging
    delete from staging.hospital h
    where h.processed_flag = 0
      and (
           h.zip_code is null 
           or length(btrim(h.zip_code)) != 5
           or not h.zip_code ~ '^[0-9]{5}$'
      );

    -- log if any bad zip rows were moved
    if v_zip_bad_count > 0 then
        perform etl.log_etl_event(
            'scrub_hospital_staging',
            'staging.hospital',
            'scrub',
            v_zip_bad_count,
            'warning',
            format(
                'moved %s rows with invalid zip codes to staging.hospital_infirmary',
                v_zip_bad_count
            ),
            null
        );
    end if;

    -- ensure remaining rows match out fit there 
    update staging.hospital h
    set zip_code = substring(btrim(h.zip_code) from 1 for 5)
    where h.processed_flag = 0
      and h.zip_code is not null
      and h.zip_code ~ '^[0-9]{5,}$';

  -- update emergency flags per the emergency services values given
    update staging.hospital h
    set emergency_flag = m.standardized_flag
    from map.emergency_service_map m
    where h.emergency_services = m.source_value
      and h.processed_flag = 0;

    --reject the record and put into infirmary if it hits this
    --what this is basically telling us is to capture rows that have a map
    update staging.hospital h
    set emergency_flag = null
    where h.processed_flag = 0
      and h.emergency_flag is distinct from 1
      and h.emergency_flag is distinct from 0;

    --we need to handle staging.hospital_infirmary
    with bad_emergency_rows as (
        select h.*,
               'invalid emergency flag value found, unmapped source value' as reason
        from staging.hospital h
        where h.processed_flag = 0
		--if we were not able to extract 1 or 0 from the source value then go ahead and reject this
          and h.emergency_flag is distinct from 1 
          and h.emergency_flag is distinct from 0
    ),
    moved_emergency as (
        insert into staging.hospital_infirmary (row_data, reason)
        select to_jsonb(br.*), br.reason
        from bad_emergency_rows br
        returning *
    )
	select count(*) into v_rows_bad
    from moved_emergency;
	
	-- delete those bad rows from staging, once removed from staging table
	delete from staging.hospital h
    where h.processed_flag = 0
		and h.emergency_flag is distinct from 1
		and h.emergency_flag is distinct from 0;
		
	-- log if any bad rows were moved
    if v_rows_bad > 0 then
        perform etl.log_etl_event(
            'scrub_county_staging',
            'staging.county',
            'scrub',
            v_rows_bad,
            'warning',
            format(
                'moved %s rows with unmapped emergency_flag values to staging.hospital_infirmary',
                v_rows_bad
            ),
            null
        );
    end if;
	

    -- birthing destination mapping
    update staging.hospital h
    set birthing_friendly_flag = m.standardized_flag
    from map.birthing_friendly_map m
    where h.birthing_friendly_designation = m.source_value
      and h.processed_flag = 0;
	  
   --if the birthing friendly designation not match from the calues in the lookup go ahead and get rid of those too
 -- Handle unmapped birthing friendly values
    with bad_birthing_rows as (
        select h.*,
               'invalid birthing friendly designation, unmapped source value' as reason
        from staging.hospital h
        where h.processed_flag = 0
          and h.birthing_friendly_flag is distinct from 1
          and h.birthing_friendly_flag is distinct from 0
          and h.birthing_friendly_designation is not null
    ),
    moved_birthing as (
        insert into staging.hospital_infirmary (row_data, reason)
        select to_jsonb(bbr.*), bbr.reason
        from bad_birthing_rows bbr
        returning *
    )
    select count(*) into v_birthing_bad_count
    from moved_birthing;
    
    -- delete unmapped birthing friendly rows from staging
    delete from staging.hospital h
    where h.processed_flag = 0
      and h.birthing_friendly_flag is distinct from 1
      and h.birthing_friendly_flag is distinct from 0
      and h.birthing_friendly_designation is not null;
    
    -- log if any bad birthing rows were moved
    if v_birthing_bad_count > 0 then
        perform etl.log_etl_event(
            'scrub_hospital_staging',
            'staging.hospital',
            'scrub',
            v_birthing_bad_count,
            'warning',
            format(
                'moved %s rows with unmapped birthing friendly values to staging.hospital_infirmary',
                v_birthing_bad_count
            ),
            null
        );
    end if;

    -- hospital ownership buckets
    update staging.hospital h
    set ownership_group = case
        when h.hospital_ownership is null or btrim(h.hospital_ownership) = '' then null
        else coalesce(
            (select ownership_group 
             from map.hospital_ownership_map 
             where lower(h.hospital_ownership) like '%' || match_text || '%' 
             limit 1),
            'OTHER'
        )
    end
    where h.processed_flag = 0;
	  
	--now we need to handle normalized hospital type
    update staging.hospital h
    set hospital_type_normalized = case
        when h.hospital_type is null or btrim(h.hospital_type) = '' then null
        else coalesce(
            (select ht.hospital_type
             from map.hospital_type_map ht
             where lower(h.hospital_type) like '%' || lower(ht.match_text) || '%'
             limit 1),
            'OTHER'
        )
    end
    where h.processed_flag = 0;
    
	  
	--generate a record hash based on the set of columns that are var to change
    update staging.hospital
    set record_hash = encode(
        digest(
            concat_ws('|',
			facility_id,
            emergency_flag,
            birthing_friendly_flag,
            hospital_overall_rating
            ),
            'sha256'
        ),
        'hex'
    )
	where processed_flag = 0;
	
    -- mark rows as processed
    update staging.hospital h
    set processed_flag = 1,
        processed_date = now()
    where h.processed_flag = 0;

    -- get row count of rows scrubbed
    get diagnostics v_rows_processed = row_count;

    -- log success
    perform etl.log_etl_event(
        'scrub_hospital_staging',
        'staging.hospital',
        'scrub',
        v_rows_processed,
        'success',
        'Completed hospital staging scrub.',
        null
    );
exception
    when others then
        -- log error
        perform etl.log_etl_event(
            'scrub_hospital_staging',
            'staging.hospital',
            'scrub',
            0,
            'error',
            'Error in hospital scrub: ' || SQLERRM,
            null
        );
        raise;
end;
$$;
