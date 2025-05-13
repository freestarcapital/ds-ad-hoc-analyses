-- ab_test__prebid_a9_adx_ebda raw query without staging/int layer

DECLARE ddate DATE DEFAULT DATE('2024-11-22');

CREATE OR REPLACE TABLE `streamamp-qa-239417.DAS_increment.AB_test_session_count` AS

-- prebid data
with bwr as (
	select
		date(TIMESTAMP_TRUNC(_PARTITIONTIME, DAY)) as record_date
		, date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst
    , site_id
		, placement_id as adunit
		, test_name
		, test_group
		, session_id
		, count(*) as impressions
    , cast(coalesce(sum(cpm)/1e7, 0) as float64) as gross_revenue

	from `freestar-157323.prod_eventstream.bidswon_raw`

    where ((_PARTITIONDATE = ddate) OR (_PARTITIONDATE = DATE_ADD(ddate, interval 1 day)) OR (_PARTITIONDATE = DATE_SUB(ddate, interval 1 day)))

	group by
			1,2,3,4,5,6,7
)
-- auction_end_raw data that have test_name present
-- required to get requests + test info for dtf dataset
, aer as (
  select
    date(timestamp_trunc(_PARTITIONTIME, day)) as record_date
    , date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) as record_date__mst
    , site_id
		, placement_id as adunit
		, test_name
		, test_group
		, session_id
    , count(*) as requests
    , sum(if(is_empty is FALSE, 1, 0)) as impressions

    from `freestar-157323.prod_eventstream.auction_end_raw`

    where ((_PARTITIONDATE = ddate) OR (_PARTITIONDATE = DATE_ADD(ddate, interval 1 day)) OR (_PARTITIONDATE = DATE_SUB(ddate, interval 1 day)))
    and test_name is not null

  group by
      1,2,3,4,5,6,7
)
-- to help get sessions from controlled experiments. filtering for sessions that are attributed to only one test_group - not both
, auction_end_raw__test as (
  select
    record_date__utc
    , record_date__mst
    , test_name
    , test_group
    , adunit
    , session_id
    , requests
    , n

    from (
        select
            record_date as record_date__utc
            , record_date__mst
            , test_name
            , test_group
            , adunit
            , session_id
            , requests
            , count(distinct test_group) over(partition by record_date, test_name, session_id) as n

        from aer
    ) x

  where x.n = 1
)
-- adding metrics ad_requests on session_id. only include sessions found in auction_end_raw__test__bwr
, prebid_final as (
    select
        bwr.record_date__mst
        , bwr.site_id
        , bwr.adunit
        , bwr.test_name
        , bwr.test_group
        , 'prebid' as inventory_platform
        , bwr.session_id
        , sum(aer.requests) as requests
        , sum(bwr.impressions) as impressions
        , sum(bwr.gross_revenue) as gross_revenue

    from bwr
    inner join auction_end_raw__test aer on aer.record_date__utc = bwr.record_date
      and aer.record_date__mst = bwr.record_date__mst
      and aer.session_id = bwr.session_id
      and aer.test_name = bwr.test_name
      and aer.test_group = bwr.test_group
      and aer.adunit = bwr.adunit

    group by 1,2,3,4,5,6,7
),

prebid_final_with_pagehits as
(
    select phr.record_date__mst, phr.site_id, adunit, phr.test_name, phr.test_group, inventory_platform, session_id, requests, impressions, gross_revenue

    from (
            select session_id, min(date_trunc(date(timestamp_millis(server_time), 'MST'), DAY)) as record_date__mst,
                min(site_id) site_id, min(test_name) test_name, min(test_group) test_group,
            from `freestar-157323.prod_eventstream.pagehits_raw`
             where ((_PARTITIONDATE = ddate) OR (_PARTITIONDATE = DATE_ADD(ddate, interval 1 day)) OR (_PARTITIONDATE = DATE_SUB(ddate, interval 1 day)))

            group by 1
         ) phr
    left join prebid_final pf using (session_id, test_name, test_group, record_date__mst, site_id)
)

-- ADX and EDBDA
, adx_ebda as (
	select
        m.EventDateMST as record_date__mst
        , m.AdUnitId as adunit_id
        , m.fs_session_id as session_id
        , sum(m.impression) as impressions
        , sum(m.unfilled) as unfilled_impressions
        , sum(m.EstimatedBackfillRevenue) as gross_revenue

  from `freestar-prod.data_transfer.NetworkBackfillImpressions` m

    where ((m.EventDateMST = ddate) OR (m.EventDateMST = DATE_ADD(ddate, interval 1 day)) OR (m.EventDateMST = DATE_SUB(ddate, interval 1 day)))

    and m.fs_session_id is not null

  group by 1,2,3
)
-- amazon TAM + UAM data
, a9 as (
	select
        m.EventDateMST as record_date__mst
        , m.AdUnitId as adunit_id
        , m.fs_session_id as session_id
        , sum(m.impression) as impressions
        , sum(m.unfilled) as unfilled_impressions
        , sum(case
            when l.CostType="CPM"
              then l.CostPerUnitInNetworkCurrency/1000
            else 0 end) as gross_revenue

  from `freestar-prod.data_transfer.NetworkImpressions` m
    left join `freestar-prod.data_transfer.match_line_item_15184186` l on l.Id = m.LineItemId
      and l.date = m.EventDateMST

    where ((m.EventDateMST = ddate) OR (m.EventDateMST = DATE_ADD(ddate, interval 1 day)) OR (m.EventDateMST = DATE_SUB(ddate, interval 1 day)))


      and m.fs_session_id is not null
      and (
          m.LineItemId = 0
          or REGEXP_CONTAINS(l.Name, 'A9 ')
      )

  group by 1,2,3
)
-- union both dtf
, us_gam_dtf_union as (
  select
    record_date__mst
    , adunit_id
    , session_id
    , impressions
    , unfilled_impressions
    , gross_revenue

  from adx_ebda

  union all

  select
    record_date__mst
    , adunit_id
    , session_id
    , impressions
    , unfilled_impressions
    , gross_revenue

  from a9
)
-- to help get sessions and test indo from controlled experiments. filtering for sessions that are attributed to only one test_group - not both
, auction_end_raw__test_dtf as (
  select
    record_date__mst
    , test_name
    , test_group
    , adunit
    , session_id
    , n

    from (
    select distinct
      record_date__mst
      , test_name
      , test_group
      , session_id
      , adunit
      , count(distinct test_group) over(partition by record_date__mst, test_name, session_id) as n

    from aer
    ) x

  where x.n = 1
)
-- getting site_id from uk and us gam mapping table - null site_id are AMP/APP
, dtf_final as (
  select
    m.record_date__mst
    , am.site_id
    , case
        when a.Name like '%jcpenney%'
          then 'jcpenney'
        else a.Name end as adunit
    , aer.test_name
    , aer.test_group
    , 'us_gam_dtf__amazon_adx_ebda' as inventory_platform
    , m.session_id
    , sum(m.impressions) + sum(m.unfilled_impressions) as requests
    , sum(m.impressions) as impressions
    , sum(m.gross_revenue) as gross_revenue

  from us_gam_dtf_union m
    left join `freestar-prod.data_transfer.match_ad_unit_15184186` a on a.Id = m.adunit_id
      and a.date = m.record_date__mst
    left join `freestar-prod.NDR_resources.gam_ad_units_map` am on am.ad_unit_name = (case
                                                                                          when a.Name like '%jcpenney%'
                                                                                            then 'jcpenney'
                                                                                          else a.Name end)
    inner join auction_end_raw__test aer on aer.record_date__mst = m.record_date__mst
      and aer.session_id = m.session_id
      and aer.adunit = a.Name

    group by 1,2,3,4,5,6,7
)
-- union both prebid and dtf
, bwr_us_gam_dtf as (
  select
    record_date__mst
    , site_id
    , adunit
    , test_name
    , test_group
    , inventory_platform
    , requests
    , session_id
    , impressions
    , gross_revenue

  from dtf_final

  union all

  select
    record_date__mst
    , site_id
    , adunit
    , test_name
    , test_group
    , inventory_platform
    , requests
    , session_id
    , impressions
    , gross_revenue

  from prebid_final_with_pagehits
)
-- computing deduplicated session count without adunit - note, de-duplicated by site_id, test_name, test_group level only
, deduped_sessions as (
  select
      m.record_date__mst
      , m.site_id
      , m.test_name
      , m.test_group
      , approx_count_distinct(m.session_id) as sessions_deduped

  from bwr_us_gam_dtf m

  group by 1,2,3,4
)
-- including collection_id and methodology from test_name and aggregating session_id
, final as (
    select
        m.record_date__mst
        , m.site_id
        , m.adunit
        , m.test_name
        , m.test_group
        , ab.collection_id
        , ab.methodology
        , m.inventory_platform
        , sum(m.requests) as requests
        , sum(m.impressions) as impressions
        , sum(m.gross_revenue) as gross_revenue
        , approx_count_distinct(m.session_id) as sessions
        , max(ds.sessions_deduped) as sessions_deduped

    from bwr_us_gam_dtf m
        left join `freestar-157323.dashboard.pubfig_ab_test` ab on ab.id = m.test_name
        left join deduped_sessions ds on ds.record_date__mst = m.record_date__mst
            and ds.site_id = m.site_id
            and ds.test_name = m.test_name
            and ds.test_group = m.test_group

    group by 1,2,3,4,5,6,7,8
)

select *

from final