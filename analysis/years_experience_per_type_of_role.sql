select
    year,
    years_professional_experience,
    count(1) filter (where has_analytics_title=true) as with_analytics_title,
    count(1) filter (where has_analytics_title=false) as without_analytics_title
from fct_all_responses
where years_professional_experience is not null
group by 1,2
order by 1 desc, 2 asc