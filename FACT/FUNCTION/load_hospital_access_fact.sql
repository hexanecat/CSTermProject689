create or replace function dbo.load_hospital_access_fact (p_full_date date)
returns void language plpgsql as $$
declare
    v_rows_inserted integer:=0;
	v_date_dim_id integer;
begin
	perform etl.log_etl_event(
	'load hospital access fact started',
	'dbo.state_ownership_access_fact',
	'',
	v_rows_inserted,
	'Starting',
	'Start hospital_access_fact snapshot for date_dim_id=' || p_full_date,
	null
);

   select date_dim_id into v_date_dim_id
   from dbo.date_dim
   where full_date = p_full_date;
   
   --bad handle it
   if v_date_dim_id is null then
      raise exception 'Date % not found in date_dim table. please use a valid date from the time dim', p_full_date;
   end if;
   
    -- Clear existing snapshot for this date
    delete
    from dbo.state_ownership_access_fact
    where date_dim_id = (select date_dim_id from dbo.date_dim where full_date = p_full_date );

    insert into dbo.hospital_access_fact (
         date_dim_id
        ,hospital_dim_id
        ,county_dim_id
		,hospital_type_dim_id
        ,risk_access_index
        ,high_risk_area
        ,availability_score
        ,quality_score
    )
    select 
         v_date_dim_id as date_dim_id
        ,h.hospital_dim_id
        ,MIN(c.county_dim_id) as county_dim_id
		,hospital_type_dim_id
        -- risk index formula on 1–10 scale
        ,round(
            least(
                greatest(
                    (
                        -- component 1: poverty rate (percent 0–100 → score 0–10)
                        (least(greatest(min(c.poverty_rate) / 100.0, 0), 1) * 10) * 0.4
                        +
                        -- component 2: income (lower income → higher score, cap at 150k)
                        ((1 - least(greatest(min(c.median_household_income) / 150000.0, 0), 1)) * 10) * 0.4
                        +
                        -- component 3: deep poverty (percent 0–100 → score 0–10)
                        (least(greatest(min(c.deep_poverty_rate) / 100.0, 0), 1) * 10) * 0.2
                    ),
                    1.0           -- floor at 1
                ),
                10.0              -- cap at 10
            ),
            2
        ) as risk_access_index
        -- High risk area flag    
        ,case 
            when MIN(c.poverty_rate) >= 0.20
             and MIN(c.median_household_income) < 50000
             and MIN(c.deep_poverty_rate) >= 0.10
            then 1 else 0
         end as high_risk_area
        ,case 
            when h.emergency_flag_2025 = TRUE then 1.0
            else 0.5
         end as availability_score
        ,(h.overall_rating_2025 * 0.7) 
            + (case when h.birthing_friendly_flag_2025 = TRUE then 1 else 0 end * 0.2)
            + (case when h.emergency_flag_2025 = TRUE then 1 else 0 end * 0.1)
            as quality_score

    from dbo.hospital_dim h
    inner join dbo.county_dim c 
      on lower(c.county_name) = lower(h.county_name)
	  and lower(c.state_code) = lower(h.state_code)
	  and c.current_flag = 'Y'
	inner join staging.hospital s
      on s.facility_id = h.facility_id
    inner join dbo.hospital_type_dim ht
      on ht.hospital_type = s.hospital_type_normalized
    group by 
         h.hospital_dim_id
        ,h.emergency_flag_2025
        ,h.overall_rating_2025
        ,h.birthing_friendly_flag_2025
		,ht.hospital_type_dim_id;
		
		
	get diagnostics v_rows_inserted = row_count;

    perform etl.log_etl_event(
        'Load hospital_access_fact',
        'dbo.hospital_access_fact',
        'I',
        v_rows_inserted,
        'success',
        'Finished hospital_access_fact snapshot for date_dim_id=' || p_full_date,
        null
    );
end;
$$;
