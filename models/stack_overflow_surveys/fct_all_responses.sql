{{ config(materialized='table') }}


with responses_2017 as (
select
    2017 as year,
    respondent,
    country,
    company_size,
    coalesce(regexp_split_to_array(replace(developer_type, ' ', ''), E';'), ARRAY['Unset']::text[]) as titles,
    years_coded_job as years_professional_experience,
    salary as annual_salary_usd,
    formal_education as education_level,
    employment_status,
    coalesce(regexp_split_to_array(i_d_e, E';'), ARRAY['Unset']::text[]) as development_environments,
    coalesce(regexp_split_to_array(have_worked_language, E';'), ARRAY['Unset']::text[]) as development_languages_used,
    coalesce(regexp_split_to_array(want_work_language, E';'), ARRAY['Unset']::text[]) as development_languages_wanted,
    coalesce(regexp_split_to_array(have_worked_database, E';'), ARRAY['Unset']::text[]) as databases_used,
    coalesce(regexp_split_to_array(want_work_database, E';'), ARRAY['Unset']::text[]) as databases_wanted
from survey_2017_responses
),

responses_2018 as (
select
    2018 as year,
    respondent,
    country,
    company_size,
    coalesce(regexp_split_to_array(dev_type, E';'), ARRAY['Unset']::text[]) as titles,
    years_coding_prof as years_professional_experience,
    converted_salary as annual_salary_usd,
    formal_education as education_level,
    employment as employment_status,
    coalesce(regexp_split_to_array(i_d_e, E';'), ARRAY['Unset']::text[]) as development_environments,
    coalesce(regexp_split_to_array(language_worked_with, E';'), ARRAY['Unset']::text[])  as development_languages_used,
    coalesce(regexp_split_to_array(language_desire_next_year, E';'), ARRAY['Unset']::text[])  as development_languages_wanted,
    coalesce(regexp_split_to_array(database_worked_with, E';'), ARRAY['Unset']::text[])  as databases_used,
    coalesce(regexp_split_to_array(database_desire_next_year, E';'), ARRAY['Unset']::text[])  as databases_wanted
from survey_2018_responses
),

responses_2019 as (
select
    2019 as year,
    respondent,
    country,
    org_size as company_size,
    coalesce(regexp_split_to_array(dev_type, E';'), ARRAY['Unset']::text[]) as titles,
    years_code_pro as years_professional_experience,
    converted_comp as annual_salary_usd,
    ed_level as education_level,
    employment as employment_status,
    coalesce(regexp_split_to_array(dev_environ, E';'), ARRAY['Unset']::text[])  as development_environments,
    coalesce(regexp_split_to_array(language_worked_with, E';'), ARRAY['Unset']::text[]) as development_languages_used,
    coalesce(regexp_split_to_array(language_desire_next_year, E';'), ARRAY['Unset']::text[]) as development_languages_wanted,
    coalesce(regexp_split_to_array(database_worked_with, E';'), ARRAY['Unset']::text[]) as databases_used,
    coalesce(regexp_split_to_array(database_desire_next_year, E';'), ARRAY['Unset']::text[])  as databases_wanted
from survey_2019_responses
),

all_responses as (
select * from responses_2017
union all
select * from responses_2018
union all
select * from responses_2019
)

select 
    *,
    case
        when year=2017 and titles && ARRAY['Datascientist', 'Machinelearningspecialist'] then true
        when year=2018 and titles && ARRAY['Data or business analyst', 'Data scientist or machine learning specialist'] then true
        when year=2019 and titles && ARRAY['Data scientist or machine learning specialist', 'Data or business analyst', 'Engineer, data'] then true
        else false
    end as has_analytics_title
from all_responses