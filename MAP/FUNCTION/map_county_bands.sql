begin
	update dbo.county_dim
	set poverty_band = case 
			when poverty_rate >= 0.20
				then 'High'
			when poverty_rate >= 0.10
				then 'Medium'
			else 'Low'
			end
		,income_band = case 
			when median_household_income < 50000
				then 'Below Average'
			when median_household_income <= 80000
				then 'Average'
			else 'Above Average'
			end
		,deep_poverty_band = case 
			when deep_poverty_rate >= 15.0
				then 'High Deep Poverty'
			when deep_poverty_rate >= 5.0
				then 'Moderate Deep Poverty'
			else 'Low Deep Poverty'
			end
	where current_flag = 'Y';-- SCD2: only update current rows
end;