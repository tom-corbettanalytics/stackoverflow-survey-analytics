select 
    year, 
    employment_status, 
    count(employment_status) as respondants
from fct_all_responses 
group by 1,2