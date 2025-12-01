create or replace function staging.scrub_hospital_staging ()
returns void
language plpgsql
as $$
declare
    v_rows_processed integer := 0;
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
    update staging.hospital d
    set zip_code = substring(d.zip_code from 1 for 5)
    where d.zip_code is not null
      and d.processed_flag = 0;

    -- update emergency flags per the emergency services values given
    update staging.hospital d
    set emergency_flag = m.standardized_flag
    from map.emergency_service_map m
    where d.emergency_services = m.source_value
      and d.processed_flag = 0;

    -- handle null / non-0/1 to be null (unknown)
    update staging.hospital d
    set emergency_flag = null
    where d.processed_flag = 0
      and d.emergency_flag is distinct from 1
      and d.emergency_flag is distinct from 0;

    -- birthing destination mapping
    update staging.hospital d
    set birthing_friendly_flag = m.standardized_flag
    from map.birthing_friendly_map m
    where d.birthing_friendly_designation = m.source_value
      and d.processed_flag = 0;

    -- hospital ownership buckets
    update staging.hospital d
    set ownership_group = case
                              when d.hospital_ownership is null
                                   or btrim(d.hospital_ownership) = ''
                                then null
                              else coalesce(m.ownership_group, 'OTHER')
                          end
    from (
        select facility_id,
               (
                   select ownership_group
                   from map.hospital_ownership_map
                   where lower(staging.hospital.hospital_ownership) like '%' || match_text || '%'
                   limit 1
               ) as ownership_group
        from staging.hospital
        where processed_flag = 0
    ) m
    where d.facility_id = m.facility_id
      and d.processed_flag = 0;
	  
	--now we need to handle normalized hospital type
	update staging.hospital d
    set hospital_type_normalized = case
                                       when d.hospital_type is null
                                            or btrim(d.hospital_type) = ''
                                         then null
                                       else coalesce(m.hospital_type, 'OTHER')
                                   end
    from (
        select facility_id,
               (
                   select ht.hospital_type
                   from map.hospital_type_map ht
                   where lower(staging.hospital.hospital_type) like '%' || lower(ht.match_text) || '%'
                   limit 1
               ) as hospital_type
        from staging.hospital
        where processed_flag = 0
    ) m
    where d.facility_id = m.facility_id
      and d.processed_flag = 0;
	  
	--generate a record hash based on the set of columns that are var to change
    update staging.hospital
    set record_hash = encode(
        digest(
            concat_ws('|',
			facility_id,
            ownership_group,
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
    update staging.hospital d
    set processed_flag = 1,
        processed_date = now()
    where d.processed_flag = 0;

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
end;
$$;
