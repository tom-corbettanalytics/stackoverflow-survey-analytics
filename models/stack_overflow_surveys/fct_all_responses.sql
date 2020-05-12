{{ config(materialized='table') }}

with responses_2016 as (
select 
    2016 as year,
    col_0_unnamed as respondent
from survey_2016_responses
),

responses_2017 as (
select
    2017 as year,
    respondent
from survey_2017_responses
),

responses_2018 as (
select
    2018 as year,
    respondent
from survey_2018_responses
),

responses_2019 as (
select
    2019 as year,
    respondent
from survey_2019_responses
)

select * from responses_2016
union all
select * from responses_2017
union all
select * from responses_2018
union all
select * from responses_2019