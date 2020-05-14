with students as (
select
    year,
    count(1) filter (where has_analytics_title=true) as with_analytics_titles,
    count(1) filter (where has_analytics_title=false) as without_analytics_titles,
    count(1) as total_students
from fct_all_responses
where student = true
    -- Pre 2017, it looks like students weren't allowed to answer the question
    -- about what kind of developer they identify as
    and year > 2017
group by 1
)

select
    year,
    students.with_analytics_titles::float / total_students as students_with_analytics_titles,
    students.without_analytics_titles::float / total_students as students_without_analytics_titles
from students
order by 1 asc