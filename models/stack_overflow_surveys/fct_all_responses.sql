{{ config(materialized='table') }}

--  2017 | 1 to 2 years
--  2017 | 10 to 11 years
--  2017 | 11 to 12 years
--  2017 | 12 to 13 years
--  2017 | 13 to 14 years
--  2017 | 14 to 15 years
--  2017 | 15 to 16 years
--  2017 | 16 to 17 years
--  2017 | 17 to 18 years
--  2017 | 18 to 19 years
--  2017 | 19 to 20 years
--  2017 | 2 to 3 years
--  2017 | 20 or more years
--  2017 | 3 to 4 years
--  2017 | 4 to 5 years
--  2017 | 5 to 6 years
--  2017 | 6 to 7 years
--  2017 | 7 to 8 years
--  2017 | 8 to 9 years
--  2017 | 9 to 10 years
--  2017 | Less than a year
--  2017 | 
--  2018 | 0-2 years
--  2018 | 12-14 years
--  2018 | 15-17 years
--  2018 | 18-20 years
--  2018 | 21-23 years
--  2018 | 24-26 years
--  2018 | 27-29 years
--  2018 | 3-5 years
--  2018 | 30 or more years
--  2018 | 6-8 years
--  2018 | 9-11 years

with responses_2017 as (
select
    2017 as year,
    respondent,
    country,
    company_size,
    coalesce(regexp_split_to_array(replace(developer_type, ' ', ''), E';'), ARRAY['Unset']::text[]) as titles,
    case 
        when years_coded_job is null then null
        when years_coded_job in ('Less than a year', '1 to 2 years') then '0-2 years'
        when years_coded_job in ('2 to 3 years', '3 to 4 years', '4 to 5 years') then '3-5 years'
        when years_coded_job in ('5 to 6 years', '6 to 7 years', '7 to 8 years', '8 to 9 years', '9 to 10 years') then '6-10 years'
        else '10+ years'
    end as years_professional_experience,
    salary as annual_salary_usd,
    case
        when formal_education is null or formal_education = 'I prefer not to answer' then null
        when formal_education ~ 'never completed' then 'None'
        when formal_education ~ 'Primary' then 'Primary'
        when formal_education ~ 'Secondary' then 'High School'
        when formal_education ~ 'Bachelor' then 'College'
        when formal_education ~ 'Some college' then 'Some college'
        when formal_education ~ 'Master' then 'Masters'
        when formal_education ~ 'Doctoral' or formal_education ~ 'Professional' then 'Advanced'
        else formal_education
    end as  education_level,
    employment_status,
    coalesce(regexp_split_to_array(i_d_e, E';'), ARRAY['Unset']::text[]) as development_environments,
    coalesce(regexp_split_to_array(have_worked_language, E';'), ARRAY['Unset']::text[]) as development_languages_used,
    coalesce(regexp_split_to_array(want_work_language, E';'), ARRAY['Unset']::text[]) as development_languages_wanted,
    coalesce(regexp_split_to_array(have_worked_database, E';'), ARRAY['Unset']::text[]) as databases_used,
    coalesce(regexp_split_to_array(want_work_database, E';'), ARRAY['Unset']::text[]) as databases_wanted,
    case when professional ~ 'Student' then true else false end as student
from survey_2017_responses
),

responses_2018 as (
select
    2018 as year,
    respondent,
    country,
    company_size,
    coalesce(regexp_split_to_array(dev_type, E';'), ARRAY['Unset']::text[]) as titles,
    case 
        when years_coding_prof is null then null
        when years_coding_prof in ('0-2 years') then '0-2 years'
        when years_coding_prof in ('3-5 years') then '3-5 years'
        when years_coding_prof in ('6-8 years','9-11 years') then '6-10 years'
        else '10+ years'
    end as years_professional_experience,    
    converted_salary as annual_salary_usd,
    case
        when formal_education is null or formal_education ~ 'prefer not to answer' then null
        when formal_education ~ 'never completed' then 'None'
        when formal_education ~ 'Primary' then 'Primary'
        when formal_education ~ 'Secondary' then 'High School'
        when formal_education ~ 'Bachelor' then 'College'
        when formal_education ~ 'Some college' or formal_education ~ 'Associate' then 'Some college'
        when formal_education ~ 'Master' then 'Masters'
        when formal_education ~ 'Doctoral' or formal_education ~ 'Professional'  or formal_education ~ 'doctoral' then 'Advanced'
        else formal_education
    end as  education_level,
    employment as employment_status,
    coalesce(regexp_split_to_array(i_d_e, E';'), ARRAY['Unset']::text[]) as development_environments,
    coalesce(regexp_split_to_array(language_worked_with, E';'), ARRAY['Unset']::text[])  as development_languages_used,
    coalesce(regexp_split_to_array(language_desire_next_year, E';'), ARRAY['Unset']::text[])  as development_languages_wanted,
    coalesce(regexp_split_to_array(database_worked_with, E';'), ARRAY['Unset']::text[])  as databases_used,
    coalesce(regexp_split_to_array(database_desire_next_year, E';'), ARRAY['Unset']::text[])  as databases_wanted,
    case when student ~ 'Yes' then true else false end as student
from survey_2018_responses
),

responses_2019 as (
select
    2019 as year,
    respondent,
    country,
    org_size as company_size,
    coalesce(regexp_split_to_array(dev_type, E';'), ARRAY['Unset']::text[]) as titles,
    case 
        when years_code_pro is null then null
        when years_code_pro in ('Less than 1 year', '1', '2') then '0-2 years'
        when years_code_pro in ('3', '4', '5') then '3-5 years'
        when years_code_pro in ('6', '7',' 8', '9', '10') then '6-10 years'
        else '10+ years'
    end as years_professional_experience,        
    converted_comp as annual_salary_usd,
    case
        when ed_level is null or ed_level ~ 'prefer not to answer' then null
        when ed_level ~ 'never completed' then 'None'
        when ed_level ~ 'Primary' then 'Primary'
        when ed_level ~ 'Secondary' then 'High School'
        when ed_level ~ 'Bachelor' then 'College'
        when ed_level ~ 'Some college' or ed_level ~ 'Associate' then 'Some college'
        when ed_level ~ 'Master' then 'Masters'
        when ed_level ~ 'Doctoral' or ed_level ~ 'Professional' or ed_level ~ 'doctoral' then 'Advanced'
        else ed_level
    end as  education_level,
    employment as employment_status,
    coalesce(regexp_split_to_array(dev_environ, E';'), ARRAY['Unset']::text[])  as development_environments,
    coalesce(regexp_split_to_array(language_worked_with, E';'), ARRAY['Unset']::text[]) as development_languages_used,
    coalesce(regexp_split_to_array(language_desire_next_year, E';'), ARRAY['Unset']::text[]) as development_languages_wanted,
    coalesce(regexp_split_to_array(database_worked_with, E';'), ARRAY['Unset']::text[]) as databases_used,
    coalesce(regexp_split_to_array(database_desire_next_year, E';'), ARRAY['Unset']::text[])  as databases_wanted,
    case when student ~ 'Yes' then true else false end as student
from survey_2019_responses
),

all_responses as (
select * from responses_2017
union all
select * from responses_2018
union all
select * from responses_2019
)

{% set analytics_titles_2017 = "ARRAY['Datascientist', 'Machinelearningspecialist']" %}
{% set analytics_titles_2018 = "ARRAY['Data or business analyst', 'Data scientist or machine learning specialist']" %}
{% set analytics_titles_2019 = "ARRAY['Data scientist or machine learning specialist', 'Data or business analyst', 'Engineer, data']" %}

select 
    *,
    case
        when year=2017 and titles && {{ analytics_titles_2017 }} then true
        when year=2018 and titles && {{ analytics_titles_2018 }} then true
        when year=2019 and titles && {{ analytics_titles_2019 }} then true
        else false
    end as has_analytics_title,
    case
        when year=2017 and titles <@ {{ analytics_titles_2017 }} then true
        when year=2018 and titles <@ {{ analytics_titles_2018 }} then true
        when year=2019 and titles <@ {{ analytics_titles_2019 }} then true
        else false
    end as has_analytics_title_only
from all_responses