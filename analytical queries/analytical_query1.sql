/*
    Identify which states have the worst (lowest) access to non-profit hospitals relative to their population size.
    This would be a telling thing having a time dimension for (Which i do)
    That would allow us to look at trend data over time to see if access for specific states is worsening or improving
*/
with nonprofithospitalcounts as (
    select 
        state_dim_id,
        hospital_count as non_profit_hospital_count
    from dbo.state_ownership_access_fact
    where hospital_ownership_dim_id in (
        select hospital_ownership_dim_id
        from dbo.hospital_ownership_dim
        where ownership_group = 'NON_PROFIT'
    )
)
select  
    row_number() over (order by (n.non_profit_hospital_count * 100000.0) / s.population asc) as rank,
    s.state_code,
    s.state_name,
    n.non_profit_hospital_count,
    s.population,
    round((n.non_profit_hospital_count * 100000.0) / nullif(s.population, 0), 2) as non_profit_hospitals_per_100k
from nonprofithospitalcounts n
inner join dbo.state_dim s
    on n.state_dim_id = s.state_dim_id
order by non_profit_hospitals_per_100k asc;