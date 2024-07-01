DECLARE processing_date DATE DEFAULT "{processing_date}";

CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.DAS_bidder_avg_rps_{processing_date}` AS

with a0 as (
 select *, if(rtt_v2='default', 80, cast(rtt_v2 as int64)) rtt_v3
 from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}`
),
a1 as (
  select distinct bidder
  from a0
), a2 as (
select distinct country_code
  from a0
  where country_code != 'default'
), a3 as (
select distinct device_category, rtt_v3
  from a0
), t1 as (
  select * from a1, a2, a3
), t2 as (
 select t1.bidder, t1.country_code, t1.rtt_v3, t1.device_category, min(a0.rtt_v3) rtt_v3_match_CC,
  if(min(a0.rtt_v3) is null, 'default', t1.country_code) country_code_match
  from t1
  left join a0
  on t1.bidder=a0.bidder and t1.device_category=a0.device_category and t1.country_code=a0.country_code and t1.rtt_v3<=a0.rtt_v3
  group by 1, 2, 3, 4
), t3 as (
 select t2.bidder, t2.country_code, t2.rtt_v3, t2.device_category, country_code_match,
  if(country_code_match = 'default', min(t4.rtt_v3), rtt_v3_match_CC) rtt_v3_match
  from t2
  left join (select * from a0 where country_code = 'default') t4
  on t2.bidder=t4.bidder and t2.device_category=t4.device_category and t2.rtt_v3<=t4.rtt_v3 and country_code_match = 'default'
  group by 1, 2, 3, 4, 5, rtt_v3_match_CC
), t4 as (
  select t3.bidder, t3.country_code, t3.rtt_v3, t3.device_category, t3.country_code_match, t3.rtt_v3_match, a0.status, a0.avg_rps,
  a0.status = 'client' is_client, a0.status in ('client', 'server') is_client_or_server
  from t3
  join a0 on t3.bidder=a0.bidder and t3.device_category=a0.device_category and t3.country_code_match=a0.country_code and t3.rtt_v3_match=a0.rtt_v3
  where rtt_v3_match is not null
)
select DATE_SUB(processing_date, INTERVAL 1 DAY) date, *,
    if(is_client, row_number() over (partition by country_code, rtt_v3, device_category, is_client order by avg_rps desc), -1) client_rank,
      countif(is_client) over (partition by country_code, rtt_v3, device_category) client_bidders,
      if(is_client_or_server, row_number() over (partition by country_code, rtt_v3, device_category, is_client_or_server order by avg_rps desc), -1) client_or_server_rank,
      countif(is_client_or_server) over (partition by country_code, rtt_v3, device_category) client_or_server_bidders,
    case when device_category = 'smartphone-ios'
        then
        case when rtt_v3 = 25 then 'fast'
             when rtt_v3 = 40 then 'medium'
             else 'slow'
        end
      when device_category = 'desktop'
      then
        case when rtt_v3 = 20 then 'fast'
             when rtt_v3 = 35 then 'medium'
             else 'slow'
        end
      else
        case when rtt_v3 = 7 then 'superfast'
            when rtt_v3 = 30 then 'fast'
            when rtt_v3 = 50 then 'medium'
            else 'slow'
        end
      end rtt_category
from t4;

select * from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_avg_rps_{processing_date}`

