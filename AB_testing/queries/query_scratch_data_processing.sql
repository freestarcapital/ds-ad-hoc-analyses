
with
tg0 as (
select *
from streamamp-qa-239417.{dataset_name}.BI_AB_test_page_hits_results_transparent_floors
where test_group = 0
),
tg1 as (
select *
from streamamp-qa-239417.{dataset_name}.BI_AB_test_page_hits_results_transparent_floors
where test_group = 1
),

tot as (
select date, domain, tg0.sessions sessions_0, tg1.sessions sessions_1, tg0.rps rps_0, tg1.rps rps_1, safe_divide(tg1.rps, tg0.rps)-1 rps_uplift
from tg0
join tg1 using (date, domain, test_name_str)
order by domain, date
)
select domain, sum(sessions_0) sum_sessions_0, sum(sessions_1) sum_sessions_1,
       avg(rps_0) avg_rps_0, avg(rps_1) avg_rps_1,
       avg(rps_uplift) avg_rps_uplift, stddev(rps_uplift)/sqrt(count(*)) avg_rps_uplift_err
from tot
group by 1
