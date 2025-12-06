with band_summary as (
    select
        c.poverty_band,
        count(*)                                            as total_hospitals,
        count(*) filter (
            where h.risk_access_index >= 5
        )                                                   as very_high_risk_hospitals,
        count(*) filter (
            where h.risk_access_index >= 4
              and h.risk_access_index < 5
        )                                                   as high_risk_hospitals,
        count(*) filter (
            where h.risk_access_index >= 3
              and h.risk_access_index < 4
        )                                                   as medium_risk_hospitals,
        count(*) filter (
            where h.risk_access_index < 3
        )                                                   as low_risk_hospitals,
        round(avg(h.risk_access_index), 2)                  as avg_risk_in_band
    from dbo.hospital_access_fact h
    join dbo.county_dim c
        on h.county_dim_id = c.county_dim_id
    group by c.poverty_band
)
select
    poverty_band,
    total_hospitals,
    very_high_risk_hospitals,
    high_risk_hospitals,
    medium_risk_hospitals,
    low_risk_hospitals,
    round(100.0 * very_high_risk_hospitals / nullif(total_hospitals, 0), 1) as pct_very_high_risk,
    round(100.0 * high_risk_hospitals      / nullif(total_hospitals, 0), 1) as pct_high_risk,
    round(100.0 * medium_risk_hospitals    / nullif(total_hospitals, 0), 1) as pct_medium_risk,
    round(100.0 * low_risk_hospitals       / nullif(total_hospitals, 0), 1) as pct_low_risk,
    round(
        100.0 * (very_high_risk_hospitals + high_risk_hospitals)
        / nullif(total_hospitals, 0),
        1
    ) as pct_high_or_very_high_risk,
    avg_risk_in_band
from band_summary
order by poverty_band;