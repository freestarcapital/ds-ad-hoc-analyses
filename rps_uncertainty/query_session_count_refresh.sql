with t1 as (
select bidder, status, country_code, device_category, rtt_category, fsrefresh, sum(session_count) session_count
from `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_refresh`
where fs_testgroup = 'experiment'
and date >= '2024-8-18' and date <= '2024-8-24' and status != 'disabled'
group by 1, 2, 3, 4, 5, 6
)
select row_number() over(order by avg(session_count) desc) rank, country_code, device_category, rtt_category, fsrefresh, avg(session_count) session_count
from t1
group by 2, 3, 4, 5
order by 1
limit 100
