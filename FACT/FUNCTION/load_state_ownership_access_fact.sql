create or replace function dbo.load_state_ownership_access_fact (p_date_dim_id integer)
returns void
language plpgsql
as $$
declare
    v_rows_inserted integer;
begin
    -- clear existing snapshot for this date
    delete
    from dbo.state_ownership_access_fact
    where date_dim_id = p_date_dim_id;

    insert into dbo.state_ownership_access_fact (
          date_dim_id
        , state_dim_id
        , hospital_ownership_dim_id
        , hospital_count
        , hospitals_per_100k
        , safe_birthing_places_per_100k
        , emergency_hospital_access_per_100k
		, average_hospital_rating
    )
    select
          p_date_dim_id                         as date_dim_id
        , s.state_dim_id

        , ho.hospital_ownership_dim_id

        -- total hospitals in this state + ownership group
        , count(distinct h.hospital_dim_id)    as hospital_count

        -- hospitals per 100k population (for this ownership group)
        , round(
              (count(distinct h.hospital_dim_id)::numeric
               / nullif(s.population, 0)
              ) * 100000
          , 2
          )                                    as hospitals_per_100k

        -- birthing-friendly hospitals per 100k (for this ownership group)
        , round(
              (sum(
                   case
                       when h.birthing_friendly_flag_2025 = true then 1
                       else 0
                   end
                 )::numeric
               / nullif(s.population, 0)
              ) * 100000
          , 2
          )                                    as safe_birthing_places_per_100k

        -- emergency hospitals per 100k (for this ownership group)
        , round(
              (sum(
                   case
                       when h.emergency_flag_2025 = true then 1
                       else 0
                   end
                 )::numeric
               / nullif(s.population, 0)
              ) * 100000
          , 2
          )                                    as emergency_hospital_access_per_100k
		, AVG(h.overall_rating_2025) as average_hospital_rating

    from dbo.state_dim s
    left join dbo.hospital_dim h
        on h.state_code = s.state_code
    left join staging.hospital sh
        on sh.facility_id = h.facility_id
    left join dbo.hospital_ownership_dim ho
        on ho.ownership_group = h.ownership_group_2025
    where s.current_flag = 'Y'
    group by
          s.state_dim_id
        , s.population
        , ho.hospital_ownership_dim_id;

    get diagnostics v_rows_inserted = row_count;

    perform etl.log_etl_event(
        'load_state_ownership_access_fact',
        'dbo.state_ownership_access_fact',
        'I',
        v_rows_inserted,
        'success',
        'Loaded state ownership access snapshot for date_dim_id=' || p_date_dim_id,
        null
    );
end;
$$;
