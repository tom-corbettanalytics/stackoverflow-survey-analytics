select 
    year, 
    count(1) as respondents,
    count(case when has_analytics_title then 1 else null end) as has_analytics_title_respondents
from {{ ref('fct_all_responses') }} 
group by 1,2