-- getting sessions that are within randomised controlled trials only - include all data with more dimension/metrics: SOV requirements

CREATE OR REPLACE TABLE `streamamp-qa-239417.DAS_eventstream_session_data.DAS_ab_test`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with auction_end_raw__test as
( select date(timestamp_trunc(_PARTITIONTIME, day)) as record_date__utc ,
date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst ,
test_name ,
test_group ,
session_id ,
site_id ,
iso country_code,
count(*) as requests
from `freestar-157323.prod_eventstream.auction_end_raw`
where timestamp_trunc(_PARTITIONTIME, day) >= '{start_date}'
and timestamp_trunc(_PARTITIONTIME, day) < '{end_date}'
and test_name is not null
group by 1,2,3,4,5,6,7 )
-- cte for dtf unique lookup - require test_name, test_group fields -- filtering for sessions that are attributed to only one test_group - not both
,

auction_end_raw__test__gam as (
select record_date__mst , test_name , test_group , session_id, country_code, n
from (
select distinct record_date__mst , test_name , test_group , session_id , country_code,
count(distinct test_group) over(partition by record_date__mst, test_name, session_id) as n
from auction_end_raw__test ) x where x.n = 1 ) ,

auction_end_raw__test__bwr as (
select record_date__utc , record_date__mst , test_name , test_group , session_id , country_code, requests , n
from ( select record_date__utc , record_date__mst , test_name , test_group , session_id , country_code, requests ,
count(distinct test_group) over(partition by record_date__utc, test_name, session_id) as n
from auction_end_raw__test ) x where x.n = 1 )

-- prebid only tests ,

,bwr_tests as (

select date(_PARTITIONDATE) as record_date__utc ,
date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst ,
site_id , test_name , test_group , session_id,

count(distinct pageview_id) as pageviews , sum(cpm / 1e7) as gross_revenue ,
count(*) as impressions
from `freestar-157323.prod_eventstream.bidswon_raw`
where date(_PARTITIONDATE) >= '{start_date}' and date(_PARTITIONDATE) < '{end_date}'
and test_name is not null
group by 1,2,3,4,5,6 )

-- adding metrics ad_requests on session_id. only include sessions found in auction_end_raw__test__bwr ,

,bwr_test__cte as
( select bwr.record_date__utc , bwr.record_date__mst , bwr.site_id , bwr.test_name , bwr.test_group ,
'prebid' as inventory_platform , bwr.session_id , country_code,
sum(aer.requests) as requests , sum(bwr.impressions) as impressions ,
sum(bwr.gross_revenue) as gross_revenue from bwr_tests bwr
inner join auction_end_raw__test__bwr aer on aer.record_date__utc = bwr.record_date__utc
 and aer.record_date__mst = bwr.record_date__mst
 and aer.session_id = bwr.session_id
 and aer.test_name = bwr.test_name
 and aer.test_group = bwr.test_group group by 1,2,3,4,5,6,7, 8 )

 -- US GAM tests only (for A9/amazon, AdX, EBDA requests only) using dtf ,

 ,us_gam_dtf as (
 select m.EventDateMST as record_date__mst ,
 m.AdUnitId as adunit_id , m.fs_session_id as session_id ,
 sum(impression) as impressions , sum(unfilled) as unfilled_impressions ,
 sum(case when l.CostType="CPM" then l.CostPerUnitInNetworkCurrency/1000 else 0 end) as gross_revenue
 from `freestar-prod.data_transfer.NetworkImpressions` m left
 join `freestar-prod.data_transfer.match_line_item_15184186` l on l.Id = m.LineItemId
 and l.date = m.EventDateMST where m.EventDateMST >= '{start_date}'
 and m.EventDateMST < '{end_date}' and
 fs_session_id is not null and ( LineItemID = 0 or REGEXP_CONTAINS(l.Name, 'A9 ') )
 group by 1,2,3

 union all

 select EventDateMST as record_date__mst , AdUnitId as adunit_id , fs_session_id as session_id ,
 sum(impression) as impressions , sum(unfilled) as unfilled_impressions ,
 sum(EstimatedBackfillRevenue) as gross_revenue
 from `freestar-prod.data_transfer.NetworkBackfillImpressions`
 where EventDateMST >= '{start_date}' and EventDateMST < '{end_date}'
 and fs_session_id is not null group by 1,2,3 )

 -- getting site_id from uk and us gam mapping table - null site_id are AMP/APP -- note: same session can be seen across many different ad units ,

 ,us_gam_dtf_cte as (
 select m.record_date__mst , am.site_id , aer.test_name , aer.test_group ,
 'us_gam_dtf__amazon_adx_ebda' as inventory_platform , m.session_id , country_code,
 sum(m.impressions) + sum(m.unfilled_impressions) as requests ,
 sum(m.impressions) as impressions , sum(m.gross_revenue) as gross_revenue
 from us_gam_dtf m
 left join `freestar-prod.data_transfer.match_ad_unit_15184186` a on a.Id = m.adunit_id
 and a.date = m.record_date__mst
 left join `freestar-prod.NDR_resources.gam_ad_units_map` am
 on am.ad_unit_name = (case when a.Name like '%jcpenney%' then 'jcpenney' else a.Name end)
 inner join auction_end_raw__test__gam aer on aer.record_date__mst = m.record_date__mst
 and aer.session_id = m.session_id group by 1,2,3,4,5,6,7 )

 -- union both bwr and dtf
 , bwr_us_gam_dtf__test as (
 select record_date__mst , site_id , test_name , test_group , inventory_platform , requests , session_id , country_code,
 impressions , gross_revenue
 from us_gam_dtf_cte

 union all
 select record_date__mst , site_id , test_name , test_group , inventory_platform , requests , session_id , country_code,
 impressions , gross_revenue
 from bwr_test__cte )

 -- including collection_id and methodology from test_name and aggregating session_id

 , final as (
 select m.record_date__mst , m.site_id , m.country_code, m.test_name , m.test_group , ab.collection_id , ab.methodology ,
 m.inventory_platform , m.requests , m.impressions , m.gross_revenue ,
 count(distinct m.session_id) as sessions from bwr_us_gam_dtf__test m
 left join `freestar-157323.dashboard.pubfig_ab_test` ab on ab.id = m.test_name
where ab.name like 'DS-725 DAS%'


 group by 1,2,3,4,5,6,7,8,9,10, 11 )

 select * from final
