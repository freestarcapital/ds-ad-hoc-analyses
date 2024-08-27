with t1 as (
select bidder, status, country_code, device_category, rtt_category, sum(session_count) session_count
from `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_v1`
where fs_testgroup = 'experiment'
and date >= '2024-8-18' and date <= '2024-8-24' and status != 'disabled'
group by 1, 2, 3, 4, 5
)
select row_number() over(order by avg(session_count) desc) rank, country_code, device_category, rtt_category, avg(session_count) session_count
from t1
group by 2, 3, 4
order by 1
limit 100