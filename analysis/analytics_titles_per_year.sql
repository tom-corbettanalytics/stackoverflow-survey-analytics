with aggs as (
    select 
        year, 
        count(1) as total_respondents,
        sum(has_analytics_title::int) as analytics_titles,
        sum(has_analytics_title::int - has_analytics_title_only::int) as analytics_and_other_titles,
        sum(has_analytics_title_only::int) as analytics_titles_only
    from {{ ref('fct_all_responses') }}
    group by 1
)

select
    *,
    round(100 * aggs.analytics_titles::numeric / aggs.total_respondents, 2) as percent_analytics_titles,
    round(100 * aggs.analytics_and_other_titles::numeric / aggs.total_respondents, 2) as percent_analytics_and_other_titles,
    round(100 * aggs.analytics_titles_only::numeric / aggs.total_respondents, 2) as percent_analytics_titles_only
from aggs    