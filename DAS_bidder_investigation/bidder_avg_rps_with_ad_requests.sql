DECLARE processing_date DATE DEFAULT "{processing_date}";

CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.DAS_bidder_avg_rps_{processing_date}` AS

with a1 as (
  select distinct bidder
  from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}`
), a2 as (
select distinct country_code
  from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}`
  where country_code != 'default'
), a3 as (
select distinct device_category, rtt_v2
  from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}`
), t1 as (
  select * from a1, a2, a3
), t2 as (
select t1.bidder, t1.country_code, t1.rtt_v2, t1.device_category,
  coalesce(t3.status, t4.status, t5.status, t6.status) status,
  coalesce(t3.country_code, t4.country_code, t5.country_code, t6.country_code) country_code_match,
  coalesce(t3.rtt_v2, t4.rtt_v2, t5.rtt_v2, t6.rtt_v2) rtt_v2_match
  from t1
  left join `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}` t3
  on t1.bidder=t3.bidder and t1.device_category=t3.device_category and t1.country_code=t3.country_code and t1.rtt_v2=t3.rtt_v2
  left join (select * from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}` where rtt_v2='default') t4
  on t1.bidder=t4.bidder and t1.device_category=t4.device_category and t1.country_code=t4.country_code
  left join (select * from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}` where country_code='default') t5
  on t1.bidder=t5.bidder and t1.device_category=t5.device_category and t1.rtt_v2=t5.rtt_v2
  left join (select * from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}` where country_code='default' and rtt_v2='default') t6
  on t1.bidder=t6.bidder and t1.device_category=t6.device_category
), t7 as (
  select t2.*, t8.avg_rps
  from t2
  join `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}` t8
  on t2.bidder=t8.bidder and t2.device_category=t8.device_category and t2.status=t8.status
    and t2.country_code_match=t8.country_code and t2.rtt_v2_match=t8.rtt_v2
  where t2.status is not null
)
select DATE_SUB(processing_date, INTERVAL 1 DAY) date, *,
    row_number() over (partition by country_code, rtt_v2, device_category, status order by avg_rps desc) status_rank,
    case when device_category = 'smartphone-ios'
        then
        case when rtt_v2 = 25 then 'fast'
             when rtt_v2 = 40 then 'medium'
             else 'slow'
        end
      when device_category = 'desktop'
      then
        case when rtt_v2 = 20 then 'fast'
             when rtt_v2 = 35 then 'medium'
             else 'slow'
        end
      else
        case when rtt_v2 = 7 then 'superfast'
            when rtt_v2 = 30 then 'fast'
            when rtt_v2 = 50 then 'medium'
            else 'slow'
        end
      end rtt_category

from t7;

select *
   from `sublime-elixir-273810.ds_experiments_us.DAS_bidder_avg_rps_{processing_date}`

