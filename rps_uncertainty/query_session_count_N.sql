
with t1 as (
select bidder, status, country_code, device_category, rtt_category, fsrefresh, sum(session_count) session_count
from `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_refresh`
where fs_testgroup = 'experiment'
and date >= '2024-8-18' and date <= '2024-08-24' and status != 'disabled'
group by 1, 2, 3, 4, 5, 6
), t2 as (
  select country_code, device_category, rtt_category, fsrefresh, avg(session_count) session_count
  from t1
  group by 1, 2, 3, 4
), t3 as (
select bidder, status, country_code, device_category, rtt_category, sum(session_count) session_count
from `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_v1`
where fs_testgroup = 'experiment'
and date >= '2024-8-18' and date <= '2024-08-24' and status != 'disabled'
group by 1, 2, 3, 4, 5
), t4 as (
  select country_code, device_category, rtt_category, avg(session_count)*{mult} session_count
  from t3
  group by 1, 2, 3
), t5 as (
select bidder, status, country_code, device_category, sum(session_count)*{mult} session_count
from `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_v1`
where fs_testgroup = 'experiment'
and date >= '2024-8-18' and date <= '2024-08-24' and status != 'disabled'
group by 1, 2, 3, 4
), t6 as (
  select country_code, device_category, avg(session_count)*{mult} session_count
  from t5
  group by 1, 2
), t7 as (
select distinct device_category, country_code, rtt_category, fsrefresh
from `streamamp-qa-239417.das_daily_configs_refresh.DAS_config_uncompressed_historic_2024-08-05_30`
where date ='2024-08-05'
), t8 as (
  select count(*) as c from t7
), t9 as (
select distinct device_category, country_code, rtt_category
from `streamamp-qa-239417.das_daily_configs_refresh.DAS_config_uncompressed_historic_2024-08-05_30`
where date ='2024-08-05'
), t10 as (
  select count(*) as c from t9
), t11 as (
select distinct device_category, country_code
from `streamamp-qa-239417.das_daily_configs_refresh.DAS_config_uncompressed_historic_2024-08-05_30`
where date ='2024-08-05'
), t12 as (
  select count(*) as c from t11
)
select 'cc,dc,rtt,refresh', sum(cast(session_count>200000 as int))/avg(c),
  sum(cast(session_count>60000 as int))/avg(c), sum(cast(session_count>30000 as int))/avg(c), sum(cast(session_count>3000 as int))/avg(c)
from t2 cross join t8

union all

select 'cc,dc,rtt', sum(cast(session_count>200000 as int))/avg(c),
  sum(cast(session_count>60000 as int))/avg(c), sum(cast(session_count>30000 as int))/avg(c), sum(cast(session_count>3000 as int))/avg(c)
from t4 cross join t10

union all

select 'cc,dc', sum(cast(session_count>200000 as int))/avg(c),
  sum(cast(session_count>60000 as int))/avg(c), sum(cast(session_count>30000 as int))/avg(c), sum(cast(session_count>3000 as int))/avg(c)

from t6 cross join t12

