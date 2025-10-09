create or replace table `streamamp-qa-239417.DAS_increment.FI_timeouts_performance_raw_data_{ddate}` as

with

auction_start_raw as
(
    select *,
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(t1.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup
    from `freestar-157323.prod_eventstream.auction_start_raw` t1
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t1.kvps) kvpss
            WHERE
                kvpss in ("fs_testgroup=optimised", "fs_testgroup=optimised-static-timeout-3002")
        ) = 1
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t1.kvps) kvpss
            WHERE
                kvpss = "fsrefresh=0"
        ) = 1
),

auction_start_raw__test as (
    select distinct fs_testgroup, session_id,
    from auction_start_raw
    qualify count(distinct fs_testgroup) over(partition by session_id) = 1
),

bidswon_raw as
(
    select *
    from `freestar-157323.prod_eventstream.bidswon_raw` t1
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
),

bwr_tests as (
    select
        session_id,
        sum(cpm / 1e7) as bwr_revenue,
    from bidswon_raw
    group by 1
),

prebid__cte as (
    select *
    from auction_start_raw__test
    left join bwr_tests using (session_id)
),

-- US GAM tests only (for A9/amazon, AdX, EBDA requests only) using dtf
us_gam_dtf as (

    select
        fs_session_id as session_id,
        sum(case when l.CostType="CPM" then l.CostPerUnitInNetworkCurrency/1000 else 0 end) as gam_A9_revenue,
        0 as gam_NBF_revenue,
        0 as gam_prebid_revenue,
    from `freestar-prod.data_transfer.NetworkImpressions` m
    join `freestar-prod.data_transfer.match_line_item_15184186` l
        on l.Id = m.LineItemId and l.date = m.EventDateMST
    where m.EventDateMST = '{ddate}'
        and fs_session_id is not null
        and REGEXP_CONTAINS(l.Name, 'A9 ')
    group by 1

    union all

    select
        fs_session_id as session_id,
        0 as gam_A9_revenue,
        0 as gam_NBF_revenue,
        sum(case when l.CostType="CPM" then l.CostPerUnitInNetworkCurrency/1000 else 0 end) as gam_prebid_revenue
    from `freestar-prod.data_transfer.NetworkImpressions` m
    join `freestar-prod.data_transfer.match_line_item_15184186` l
        on l.Id = m.LineItemId and l.date = m.EventDateMST
    where m.EventDateMST = '{ddate}'
        and fs_session_id is not null
        and NOT (REGEXP_CONTAINS(l.Name, 'A9 ') or (LineItemID = 0) or (lineitemtype='HOUSE'))
    group by 1

    union all

    --if we want to break NBF data into AdSense, AdX and Open Bidding then we need to use the product field
    --CASE WHEN Product = 'AdSense' THEN 'Google AdSense' WHEN Product = 'Ad Exchange' THEN 'Google Ad Exchange' WHEN Product = 'Exchange Bidding' THEN 'Open Bidding' ELSE product END advertiser,
    select
        fs_session_id as session_id,
        0 as gam_A9_revenue,
        sum(EstimatedBackfillRevenue) as gam_NBF_revenue,
        0 as gam_prebid_revenue,
    from `freestar-prod.data_transfer.NetworkBackfillImpressions`
    where EventDateMST = '{ddate}'
    group by 1
),

-- getting site_id from uk and us gam mapping table - null site_id are AMP/APP
-- note: same session can be seen across many different ad units
us_gam_dtf_cte as (
    select
        session_id,
        sum(gam_A9_revenue) as gam_A9_revenue,
        sum(gam_NBF_revenue) as gam_NBF_revenue,
        sum(gam_prebid_revenue) as gam_prebid_revenue
    from us_gam_dtf
    group by 1
),

joined_session_data as (
    select *,
        case
            when bwr_revenue is not null then 'bwr_avail'
            when gam_NBF_revenue is not null then 'GAM_avail'
            else 'unknown'
            end as data_status
    from
    prebid__cte
    left outer join
    us_gam_dtf_cte
    using (session_id)
),

full_session_data as (
    select *,

    CASE data_status
        WHEN 'bwr_avail' THEN coalesce(bwr_revenue, 0) + coalesce(gam_A9_revenue, 0) + coalesce(gam_NBF_revenue, 0)
        WHEN 'GAM_avail' THEN coalesce(gam_prebid_revenue, 0) + coalesce(gam_A9_revenue, 0) + coalesce(gam_NBF_revenue, 0)
        ELSE 0
        END as revenue,

    CASE data_status
        WHEN 'unknown' THEN 1
        ELSE 0
        END as unknown_data_status,

    from joined_session_data
),

results as (
    select fs_testgroup,
        count(*) sessions,
        sum(revenue) revenue,
        safe_divide(sum(revenue), count(*)) * 1000 rps
    from full_session_data
    group by 1
)

select '{ddate}' date, *
from results;


{create_or_insert_statement}
select *
from `streamamp-qa-239417.DAS_increment.FI_timeouts_performance_raw_data_{ddate}`
where fs_testgroup is not null;
