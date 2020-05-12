{{ config(materialized='table') }}

/*
DEMOS
X titles
location

TOOLS
IDE
languages
database worked with

EMPLOYMENT
company size
years of experience
salary
education level
employment status


*/


with responses_2016 as (
select 
    2016 as year,
    col_0_unnamed as respondent,
    coalesce(regexp_split_to_array(replace(self_identification, ' ', ''), E';'), ARRAY['Unset']::text[]) as titles
    -- age_midpoint as age
from survey_2016_responses
),

responses_2017 as (
select
    2017 as year,
    respondent,
    coalesce(regexp_split_to_array(replace(developer_type, ' ', ''), E';'), ARRAY['Unset']::text[]) as titles
from survey_2017_responses
),

responses_2018 as (
select
    2018 as year,
    respondent,
    coalesce(regexp_split_to_array(replace(dev_type, ' ', ''), E';'), ARRAY['Unset']::text[]) as titles
from survey_2018_responses
),

responses_2019 as (
select
    2019 as year,
    respondent,
    coalesce(regexp_split_to_array(dev_type, E';'), ARRAY['Unset']::text[]) as titles
    -- age,
from survey_2019_responses
)

select * from responses_2016
union all
select * from responses_2017
union all
select * from responses_2018
union all
select * from responses_2019