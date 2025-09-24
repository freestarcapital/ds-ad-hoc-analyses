create or replace table `streamamp-qa-239417.DAS_increment.FI_AB_test_performance_raw_data_all_sites_{ddate}_explore` as

with

auction_end_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.auction_end_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
),

auction_start_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.auction_start_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
),

pagehits_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.pagehits_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
),

bidswon_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
),

page_hits_cte as
(
    select distinct session_id
    from pagehits_raw
),

auction_end_raw__test as (
    select
        session_id,
        count(*) as aer_requests,
        countif(unfilled) aer_unfilled,
        countif(is_empty) aer_is_empty,
        countif(auction_type = 'PREBID-GAM') aer_PREDBID_GAM_requests,
        countif(auction_type = 'PREBID') aer_PREBID_requests,
        countif(auction_type = 'GAM') aer_GAM_requests,
        countif(is_native_render) aer_native_render_requests,
        countif(is_gam_bypass) aer_gam_bypass_requests
    from auction_end_raw
    group by 1
),

auction_start_raw__test as (
    select
        session_id,
        count(*) as asr_requests
    from auction_start_raw
    group by 1
),

-- prebid only tests
bwr_tests as (
    select
        session_id,
        sum(cpm / 1e7) as bwr_revenue,
        sum(if(is_native_render, cpm / 1e7, 0)) as bwr_native_render_revenue,
        sum(if(is_gam_bypass, cpm / 1e7, 0)) as bwr_gam_bypass_revenue,
        count(*) as bwr_impressions,
        countif(is_native_render) bwr_native_render_impressions,
        countif(is_gam_bypass) bwr_gam_bypass_impressions
    from bidswon_raw
    group by 1
),

prebid__cte as (
    select *
    from page_hits_cte
    left join auction_start_raw__test using (session_id)
    left join auction_end_raw__test using (session_id)
    left join bwr_tests using (session_id)
),

-- US GAM tests only (for A9/amazon, AdX, EBDA requests only) using dtf
us_gam_dtf as (

    select
        fs_session_id as session_id,
        sum(impression) as gam_house_impressions, -- reported as impression, but really unfilled because house -- maybe even separate 'house_impression'
        0 as gam_LIID0_impressions,
        0 as gam_LIID0_unfilled,
        0 as gam_LIID0_revenue,
        0 as gam_A9_impressions,
        0 as gam_A9_unfilled,
        0 as gam_A9_revenue,
        0 as gam_NBF_impressions,
        0 as gam_NBF_unfilled,
        0 as gam_NBF_revenue,
        0 as gam_prebid_impressions,
        0 as gam_prebid_unfilled,
        0 as gam_prebid_revenue
    from `freestar-prod.data_transfer.NetworkImpressions` m
    join `freestar-prod.data_transfer.match_line_item_15184186` l
        on l.Id = m.LineItemId and l.date = m.EventDateMST
    where m.EventDateMST = '{ddate}'
        and fs_session_id is not null
        and l.lineitemtype = 'HOUSE'
    group by 1

    union all

    select
        fs_session_id as session_id,
        0 as gam_house_impressions,
        sum(impression) as gam_LIID0_impressions,
        sum(unfilled) as gam_LIID0_unfilled,
        0 as gam_LIID0_revenue,
        0 as gam_A9_impressions,
        0 as gam_A9_unfilled,
        0 as gam_A9_revenue,
        0 as gam_NBF_impressions,
        0 as gam_NBF_unfilled,
        0 as gam_NBF_revenue,
        0 as gam_prebid_impressions,
        0 as gam_prebid_unfilled,
        0 as gam_prebid_revenue
    from `freestar-prod.data_transfer.NetworkImpressions` m
    where m.EventDateMST = '{ddate}'
        and fs_session_id is not null
        and LineItemID = 0
    group by 1

    union all

    select
        fs_session_id as session_id,
        0 as gam_house_impressions,
        0 as gam_LIID0_impressions,
        0 as gam_LIID0_unfilled,
        0 as gam_LIID0_revenue,
        sum(impression) as gam_A9_impressions,
        sum(unfilled) as gam_A9_unfilled,
        sum(case when l.CostType="CPM" then l.CostPerUnitInNetworkCurrency/1000 else 0 end) as gam_A9_revenue,
        0 as gam_NBF_impressions,
        0 as gam_NBF_unfilled,
        0 as gam_NBF_revenue,
        0 as gam_prebid_impressions,
        0 as gam_prebid_unfilled,
        0 as gam_prebid_revenue
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
        0 as gam_house_impressions,
        0 as gam_LIID0_impressions,
        0 as gam_LIID0_unfilled,
        0 as gam_LIID0_revenue,
        0 as gam_A9_impressions,
        0 as gam_A9_unfilled,
        0 as gam_A9_revenue,
        0 as gam_NBF_impressions,
        0 as gam_NBF_unfilled,
        0 as gam_NBF_revenue,
        sum(impression) as gam_prebid_impressions,
        sum(unfilled) as gam_prebid_unfilled,
        sum(case when l.CostType="CPM" then l.CostPerUnitInNetworkCurrency/1000 else 0 end) as gam_prebid_revenue
    from `freestar-prod.data_transfer.NetworkImpressions` m
    join `freestar-prod.data_transfer.match_line_item_15184186` l
        on l.Id = m.LineItemId and l.date = m.EventDateMST
    where m.EventDateMST = '{ddate}'
        and fs_session_id is not null
        and NOT (REGEXP_CONTAINS(l.Name, 'A9 ') or (LineItemID = 0) or (lineitemtype='HOUSE'))
    group by 1

    union all

    select
        fs_session_id as session_id,
        0 as gam_house_impressions,
        0 as gam_LIID0_impressions,
        0 as gam_LIID0_unfilled,
        0 as gam_LIID0_revenue,
        0 as gam_A9_impressions,
        0 as gam_A9_unfilled,
        0 as gam_A9_revenue,
        sum(impression) as gam_NBF_impressions,
        sum(unfilled) as gam_NBF_unfilled,
        sum(EstimatedBackfillRevenue) as gam_NBF_revenue,
        0 as gam_prebid_impressions,
        0 as gam_prebid_unfilled,
        0 as gam_prebid_revenue
    from `freestar-prod.data_transfer.NetworkBackfillImpressions`
    where EventDateMST = '{ddate}'
    group by 1, 2
),

-- getting site_id from uk and us gam mapping table - null site_id are AMP/APP
-- note: same session can be seen across many different ad units
us_gam_dtf_cte as (
    select
        m.session_id,
        sum(gam_house_impressions) as gam_house_impressions,
        sum(gam_LIID0_impressions) as gam_LIID0_impressions,
        sum(gam_LIID0_unfilled) as gam_LIID0_unfilled,
        sum(gam_LIID0_revenue) as gam_LIID0_revenue,
        sum(gam_A9_impressions) as gam_A9_impressions,
        sum(gam_A9_unfilled) as gam_A9_unfilled,
        sum(gam_A9_revenue) as gam_A9_revenue,
        sum(gam_NBF_impressions) as gam_NBF_impressions,
        sum(gam_NBF_unfilled) as gam_NBF_unfilled,
        sum(gam_NBF_revenue) as gam_NBF_revenue,
        sum(gam_prebid_impressions) as gam_prebid_impressions,
        sum(gam_prebid_unfilled) as gam_prebid_unfilled,
        sum(gam_prebid_revenue) as gam_prebid_revenue
    from us_gam_dtf m
    group by 1
),

full_session_data as (

    select *,
        case
            when bwr_impressions is not null then 'bwr_avail'
            when gam_NBF_impressions is not null then 'GAM_avail'
            when aer_requests is not null then 'aer_avail'
            when asr_requests is not null then 'asr_avail'
            when (asr_requests is NULL) AND (aer_requests is NULL) AND (bwr_impressions is NULL) AND (gam_NBF_impressions is NULL) then 'nothing_avail'
            else 'unknown'
            end as data_status
    from
    prebid__cte
    full outer join
    us_gam_dtf_cte
    using (session_id)
)

select
    asr_requests is not null as asr_data,
    aer_requests is not null as aer_data,
    bwr_impressions is not null as bwr_data,
    gam_NBF_impressions is not null as gam_data,
    data_status,

    count(*) sessions,

    sum(asr_requests) as asr_requests,
    sum(aer_requests) as aer_requests,
    sum(aer_unfilled) as aer_unfilled,
    sum(aer_is_empty) as aer_is_empty,
    sum(aer_PREDBID_GAM_requests) as aer_PREDBID_GAM_requests,
    sum(aer_PREBID_requests) as aer_PREBID_requests,
    sum(aer_GAM_requests) as aer_GAM_requests,
    sum(aer_native_render_requests) as aer_native_render_requests,
    sum(aer_gam_bypass_requests) as aer_gam_bypass_requests,
    sum(bwr_revenue) as bwr_revenue,
    sum(bwr_native_render_revenue) bwr_native_render_revenue,
    sum(bwr_gam_bypass_revenue) bwr_gam_bypass_revenue,
    sum(bwr_impressions) as bwr_impressions,
    sum(bwr_native_render_impressions) as bwr_native_render_impressions,
    sum(bwr_gam_bypass_impressions) as bwr_gam_bypass_impressions,
    sum(gam_house_impressions) as gam_house_impressions,
    sum(gam_LIID0_impressions) as gam_LIID0_impressions,
    sum(gam_LIID0_unfilled) as gam_LIID0_unfilled,
    sum(gam_LIID0_revenue) as gam_LIID0_revenue,
    sum(gam_A9_impressions) as gam_A9_impressions,
    sum(gam_A9_unfilled) as gam_A9_unfilled,
    sum(gam_A9_revenue) as gam_A9_revenue,
    sum(gam_NBF_impressions) as gam_NBF_impressions,
    sum(gam_NBF_unfilled) as gam_NBF_unfilled,
    sum(gam_NBF_revenue) as gam_NBF_revenue,
    sum(gam_prebid_impressions) as gam_prebid_impressions,
    sum(gam_prebid_unfilled) as gam_prebid_unfilled,
    sum(gam_prebid_revenue) as gam_prebid_revenue,

    CASE data_status
        WHEN 'bwr_avail' THEN sum(coalesce(bwr_revenue, 0) + coalesce(gam_A9_revenue, 0) + coalesce(gam_NBF_revenue, 0))
        WHEN 'GAM_avail' THEN sum(coalesce(gam_prebid_revenue, 0) + coalesce(gam_A9_revenue, 0) + coalesce(gam_NBF_revenue, 0))
        ELSE 0
        END as revenue,

    CASE data_status
        WHEN 'bwr_avail' THEN sum(coalesce(bwr_revenue, 0))
        WHEN 'GAM_avail' THEN sum(coalesce(gam_prebid_revenue, 0))
        ELSE 0
        END as prebid_revenue,

    CASE data_status
        WHEN 'bwr_avail' THEN sum(coalesce(bwr_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) - coalesce(gam_house_impressions, 0))
        WHEN 'GAM_avail' THEN sum(coalesce(gam_prebid_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) - coalesce(gam_house_impressions, 0))
        WHEN 'aer_avail' THEN sum(coalesce(aer_requests, 0) - coalesce(aer_unfilled, 0))
        ELSE 0
        END as impressions,

    CASE data_status
        WHEN 'bwr_avail' THEN sum(coalesce(bwr_impressions, 0))
        WHEN 'GAM_avail' THEN sum(coalesce(gam_prebid_impressions, 0))
        WHEN 'aer_avail' THEN sum(coalesce(aer_requests, 0) - coalesce(aer_unfilled, 0))
        ELSE 0
        END as prebid_impressions,

    CASE data_status
        WHEN 'bwr_avail' THEN sum(coalesce(aer_unfilled, 0) + coalesce(gam_house_impressions, 0))
        WHEN 'GAM_avail' THEN sum(coalesce(gam_LIID0_unfilled, 0) + coalesce(gam_house_impressions, 0))
        WHEN 'aer_avail' THEN sum(coalesce(aer_unfilled, 0))
        WHEN 'asr_avail' THEN sum(coalesce(asr_requests, 0))
        ELSE 0
        END as unfilled,

    CASE data_status
        WHEN 'bwr_avail' THEN sum(coalesce(bwr_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) + coalesce(aer_unfilled, 0))
        WHEN 'GAM_avail' THEN sum(coalesce(gam_prebid_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) + coalesce(gam_LIID0_unfilled, 0))
        WHEN 'aer_avail' THEN sum(coalesce(aer_requests, 0))
        WHEN 'asr_avail' THEN sum(coalesce(asr_requests, 0))
        ELSE 0
        END as requests,

    CASE data_status
        WHEN 'unknown' THEN count(*)
        ELSE 0
        END as unknown_data_status_count

from full_session_data
group by 1, 2, 3, 4, 5;

