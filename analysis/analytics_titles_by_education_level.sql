with agg as (
select
    year, 
    count(1) filter (where has_analytics_title=true) as with_analytics_roles,
    count(1) filter (where has_analytics_title=false) as without_analytics_roles,
    count(1) filter (where has_analytics_title=true and education_level in ('Masters', 'Advanced')) as with_analytics_roles_and_adv_degrees,
    count(1) filter (where has_analytics_title=false and education_level in ('Masters', 'Advanced')) as without_analytics_roles_and_adv_degrees    
from fct_all_responses 
group by 1
)

select
    agg.year,
    agg.with_analytics_roles_and_adv_degrees::numeric / agg.with_analytics_roles as percent_analytics_with_advanced_degrees,
    agg.without_analytics_roles_and_adv_degrees::numeric / agg.without_analytics_roles as percent_without_analytics_with_advanced_degrees
from agg    
