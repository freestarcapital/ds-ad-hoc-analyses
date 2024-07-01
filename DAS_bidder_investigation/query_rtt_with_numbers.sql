
DECLARE processing_date DATE DEFAULT "{processing_date}";

CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.DAS_bidder_investigation_{processing_date}_{day_interval}` AS

with base as (
    SELECT date, bidder,country_code, rtt_category,
    `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
    status, session_count, revenue,impressions,unfilled,
    FROM freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_expanded_v1
    WHERE DATE>= DATE_SUB(processing_date, INTERVAL {day_interval} DAY) AND DATE<= DATE_SUB(processing_date, INTERVAL 1 DAY)
    and fs_testgroup = 'experiment'
    and country_code is not null
    and status != 'disabled'
    ),

aggregate as (
  SELECT bidder,country_code,
  device_category, rtt_category,
  status,sum(revenue) revenue, sum(session_count) session_count,sum(impressions+unfilled) ad_requests
  from base
  group by bidder, country_code, device_category ,rtt_category,  status
  ),

country_code_default as (
  SELECT bidder,
  CASE WHEN min(session_count) over(partition by bidder,country_code, device_category,rtt_category) >= 25 THEN country_code ELSE 'default' END country_code,
  device_category, rtt_category,status,revenue, session_count, ad_requests
  from aggregate
),


second_aggregate as (
  SELECT bidder,country_code,
  device_category, rtt_category,status,
  sum(revenue) revenue, sum(session_count) session_count,sum(ad_requests) ad_requests
  from country_code_default
  group by 1,2,3,4,5
  ),


top_cohorts_all as(
  SELECT device_category, country_code, bidder, rtt_category,status,
  sum(revenue) revenue, sum(session_count) session_count, safe_divide(sum(revenue), sum(session_count))*1000 as rps,safe_divide(sum(revenue), sum(ad_requests))*1000 as cpma,
  case when status='server' then safe_divide(sum(revenue), sum(session_count))*1000 else 0 end as server_side_rps,
  FROM second_aggregate
  group by  device_category, country_code, bidder,rtt_category, status
  ),

remove_low_cpma_server as(
  select *
  from top_cohorts_all
  where not (cpma<0.10 and status='server')
  ),

top_cohorts as(
  select *,
  row_number() over (partition by country_code,device_category, bidder, rtt_category order by safe_divide(revenue,session_count) desc) rn
  from remove_low_cpma_server
  ),

top_cohorts_with_diff AS (
  select bidder, device_category,country_code,rtt_category, status, revenue,session_count,rn,
	safe_divide(ABS((SELECT rps FROM top_cohorts sub WHERE rn=1 AND sub.country_code=top_cohorts.country_code
  AND sub.bidder = top_cohorts.bidder AND sub.device_category =  top_cohorts.device_category AND sub.rtt_category =  top_cohorts.rtt_category) - rps ), rps) diff, rps,
  server_side_rps
  from top_cohorts
  ),


-- Due to the filter within the CTE below, there might be edge cases where we will have 2 rows per cohort,
-- in which server rps within '{perc}' = 1% difference from the winner status. In this case we choose the MAX(status) since
-- letter "S" of server is the maximum of all other statuses. We again choose min(rps) to select the server's rps value due to the same reason.
-- So it could be the case that status=server and server_side_rn=2, so we need the logic in add_fallback_server_rps to resolve that

top_cohorts_filtered AS (
  select a.bidder, a.device_category,a.country_code,a.rtt_category,
  sum(a.revenue) revenue,sum(a.session_count) session_count,  MAX(a.status) status,
  ROUND(min(a.rps),2) AS avg_rps, round(avg(b.server_side_rps),4) server_side_rps, avg(b.rn) server_side_rn
	from top_cohorts_with_diff a
  left join (select * from top_cohorts_with_diff where status = 'server') b using (bidder, device_category, country_code,rtt_category)
	WHERE (a.rn=1 or (a.status='server' and a.diff< {perc}))
	group by bidder, device_category, country_code,rtt_category
	),

add_fallback_server_rps as(
  select bidder, device_category,country_code,rtt_category, status, revenue,session_count,avg_rps,server_side_rps, server_side_rn,
  case when server_side_rn=2 and status!='server' then server_side_rps else null end fallback_rps
  from top_cohorts_filtered
  ),


rtt_ranking as (
  select *,
  case
      when rtt_category = 'superfast' then 4
      when rtt_category = 'fast'  then 3
      when rtt_category = 'medium' then 2
      when rtt_category = 'slow' then 1
  end as rtt_rank
  from add_fallback_server_rps
  where rtt_category is not null
  ),

step1 as (
  select * ,
  ROW_NUMBER() OVER (PARTITION BY bidder, device_category,country_code) rn
  from rtt_ranking
  where session_count>=25
  ),

multiple_entry_cohorts as  (
  select * from step1
  qualify count(*) over (PARTITION BY bidder, device_category,country_code) >1
  ),

rtt_default as (
  select a.bidder,a.device_category,a.country_code, a.revenue,a.session_count,
  case when min(rtt_rank) over(partition by device_category,country_code,bidder) = rtt_rank then 'default'
  else rtt_category end rtt_category,
  a.status, a.rtt_rank, a.rn, a.avg_rps, a.fallback_rps
  from multiple_entry_cohorts as a
  ),


comparison as (
  select bidder, device_category,country_code,rtt_category,status,rtt_rank,revenue,session_count,
  lag(status) over(PARTITION BY bidder, device_category,country_code order by rtt_rank) as prev,
  lead(status) over(PARTITION BY bidder, device_category,country_code order by rtt_rank) as next,
  avg_rps,fallback_rps,
  lead(fallback_rps) over(partition by bidder, device_category,country_code order by rtt_rank) next_fallback_rps
  from rtt_default
  ),

fallback_rps_diff as(
  select *,
  ifnull(abs(safe_divide((fallback_rps - next_fallback_rps),next_fallback_rps))*100,0) fallback_rps_diff
  from comparison
  ),

new_rps as(
  select bidder, device_category,country_code,rtt_category,status,prev,rtt_rank,
  case when ((status=prev) or (status=next)) and fallback_rps_diff<{fallback_rps_perc} then sum(revenue) over(partition by bidder, device_category,country_code,status) else revenue end revenue,
  case when ((status=prev) or (status=next)) and fallback_rps_diff<{fallback_rps_perc} then sum(session_count) over(partition by bidder, device_category,country_code,status) else session_count end session_count,
  avg_rps,fallback_rps,next_fallback_rps,fallback_rps_diff
  from fallback_rps_diff c
  ),

final_rtt as (
  select bidder, device_category,country_code,rtt_category,status,rtt_rank,revenue,session_count, safe_divide(revenue,session_count)*1000 avg_rps,
  fallback_rps,fallback_rps_diff
  from new_rps c
  where rtt_category = 'default' or status != prev or fallback_rps_diff>={fallback_rps_perc}
  ),

single_entry_cohorts as  (
  select * except(rtt_category), 'default' rtt_category,
  from step1
  qualify count(*) over (PARTITION BY bidder, device_category,country_code) =1
  ),

all_cohorts as (
  select bidder, device_category, country_code, rtt_category, status,rtt_rank, avg_rps,fallback_rps  from single_entry_cohorts
  union all
  select bidder, device_category, country_code, rtt_category, status,rtt_rank, avg_rps,fallback_rps  from final_rtt
  ),

reverse as (
  select bidder, device_category, country_code ,
  case when (device_category) = 'smartphone-ios'
  then
    case when rtt_rank = 3 then 25
        when rtt_rank = 2 then 40
        when rtt_rank = 1 then 80
    end
  when (device_category) = 'desktop'
  then
    case when rtt_rank = 3 then 20
        when rtt_rank = 2 then 35
        when rtt_rank = 1 then 80
    end
    else
      case when rtt_rank = 4 then 7
        when rtt_rank = 3 then 30
        when rtt_rank = 2 then 50
        else 80
    end
  end as rtt_v2,
  rtt_category,rtt_rank,
  status, avg_rps,fallback_rps
  from all_cohorts
  )

select r.bidder, r.device_category, r.country_code,
case when min(rtt_rank) over (partition by device_category,country_code,bidder) = rtt_rank then 'default'
else cast(rtt_v2 as string)
end rtt_v2,
r.status,
case when status='off' then null else round(r.avg_rps,2) end avg_rps, round(fallback_rps,2) fallback_rps
from reverse r
order by 1,2,3,4,5

