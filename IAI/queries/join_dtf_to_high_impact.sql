DECLARE ddates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE('2024-11-11'), DATE('2024-11-18'));

with ar as (
  select distinct auction_id as fs_auction_id
  from `freestar-157323.prod_eventstream.iai_high_impact_ad_request`
  WHERE _PARTITIONDATE in UNNEST(ddates)
), j as (
  select dtf.*, if(ar.fs_auction_id is not null, revenue, 0) revenue_high_impact,
  from `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data` dtf
  left join ar using(fs_auction_id)
)
select date, count(distinct fs_session_id) sessions,
count(distinct fs_auction_id) auctions, sum(revenue) revenue,
sum(revenue_high_impact) revenue_high_impact,
sum(if((lower(ad_unit_name) like '%dynamic%'), revenue, 0)) revenue_dynamic,
sum(if((lower(ad_unit_name) like '%iai%'), revenue, 0)) revenue_iai,
sum(if((lower(ad_unit_name) like '%dynamic%') or (lower(ad_unit_name) like '%iai%'), revenue, 0)) revenue_dynamic_iai,
sum(impressions) impressions, sum(unfilled) unfilled,
sum(revenue) / count(distinct fs_session_id) *1000 rps
from j
group by 1
order by 1




DECLARE ddates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE('2024-11-11'), DATE('2024-11-18'));

with ar as (
  select distinct auction_id as fs_auction_id
  from `freestar-157323.prod_eventstream.iai_high_impact_ad_request`
  WHERE _PARTITIONDATE in UNNEST(ddates)
), j as (
  select dtf.*, if(ar.fs_auction_id is null, 1, 0) fs_auction_id_not_in_ar,
    if(ar.fs_auction_id is not null, revenue, 0) revenue_high_impact,
  from `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data` dtf
  left join ar using(fs_auction_id)
)
select fs_auction_id_not_in_ar, count(*), sum(revenue) --ad_unit_name, revenue_high_impact = 0, count(*)
from j
where --revenue_high_impact = 0
--and
ad_unit_name = 'droid-life-dynamic_inarticle_iai'
--and revenue > 0
and date = '2024-11-14'
group by 1


DECLARE ddates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE('2024-11-11'), DATE('2024-11-18'));

with ar as (
  select distinct session_id as fs_session_id
  from `freestar-157323.prod_eventstream.iai_high_impact_ad_request`
  WHERE _PARTITIONDATE in UNNEST(ddates)
), j as (
  select dtf.*, if(ar.fs_session_id is null, 1, 0) fs_session_id_not_in_ar,
    if(ar.fs_session_id is not null, revenue, 0) revenue_high_impact,
  from `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data` dtf
  left join ar using(fs_session_id)
)
select fs_session_id_not_in_ar, count(*), sum(revenue) --ad_unit_name, revenue_high_impact = 0, count(*)
from j
where --revenue_high_impact = 0
--and
ad_unit_name = 'droid-life-dynamic_inarticle_iai'
--and revenue > 0
and date = '2024-11-14'
group by 1