select 
    year, 
    percentile_disc(0.5) within group (order by annual_salary_usd) filter (where has_analytics_title=true) as with_analytics_roles,
    percentile_disc(0.5) within group (order by annual_salary_usd) filter (where has_analytics_title=false) as without_analytics_roles
from {{ ref('fct_all_responses') }}
where country = 'United States'
group by 1