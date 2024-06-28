

DECLARE processing_date DATE DEFAULT "{processing_date}";

CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.DAS_bidder_avg_rps_rtt_category_{processing_date}` AS

with a1 as (
  select distinct bidder
  from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_inv_rtt_raw_{processing_date}`
), a2 as (
select distinct country_code
  from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_inv_rtt_raw_{processing_date}`
  where country_code != 'default'
), a3 as (
select distinct rtt_category_raw
  from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_inv_rtt_raw_{processing_date}`
), a4 as (
select distinct device_category
  from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_inv_rtt_raw_{processing_date}`
), t1 as (
  select * from a1, a2, a3, a4
), t2 as (
select t1.bidder, t1.country_code, t1.rtt_category_raw, t1.device_category,
    coalesce(t2.avg_rps, t3.avg_rps) avg_rps from t1
left join (select * from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_inv_rtt_raw_{processing_date}` where status='client') t2
  on t1.bidder=t2.bidder and t1.device_category=t2.device_category and t1.country_code=t2.country_code and t1.rtt_category_raw=t2.rtt_category_raw
left join (select * from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_inv_rtt_raw_{processing_date}` where status='client' and country_code='default') t3
  on t1.bidder=t3.bidder and t1.device_category=t3.device_category and t1.rtt_category_raw=t3.rtt_category_raw
), t3 as (
select *, row_number() over(partition by country_code, device_category, rtt_category_raw order by avg_rps desc) bidder_rank
from t2
where avg_rps is not null
), t4 as (
SELECT bidder, country_code, rtt_category rtt_category_raw,
   `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
   sum(impressions+unfilled) ad_requests

   FROM freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_v1
   WHERE DATE>= DATE_SUB(processing_date, INTERVAL {day_interval} DAY) AND DATE<= DATE_SUB(processing_date, INTERVAL 1 DAY)
   and fs_testgroup = 'optimised'
   and country_code is not null
   and status != 'disabled'
   and rtt_category is not null
   group by 1, 2, 3, 4
)
select DATE_SUB(processing_date, INTERVAL 1 DAY) date, bidder, country_code, rtt_category_raw, device_category,
    bidder_rank, ad_requests
from t3
join t4 using (bidder, country_code, rtt_category_raw, device_category);

select * from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_avg_rps_rtt_category_{processing_date}`
