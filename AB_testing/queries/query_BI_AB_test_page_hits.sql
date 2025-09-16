create or replace table `streamamp-qa-239417.DAS_increment.BI_AB_raw_page_hits_{name}_{ddate}` as

with

auction_end_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.auction_end_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
        and NET.REG_DOMAIN(page_url) in {domain_list}
),

auction_start_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.auction_start_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
        and NET.REG_DOMAIN(page_url) in {domain_list}
),

pagehits_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.pagehits_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
        and NET.REG_DOMAIN(page_url) in {domain_list}
),

bidswon_raw as
(
    select * except (test_name), coalesce(test_name, 'null') test_name_str
    from `freestar-157323.prod_eventstream.bidswon_raw`
    where _PARTITIONDATE >= date_sub('{ddate}', interval 1 day)
        and _PARTITIONDATE <= date_add('{ddate}', interval 1 day)
        and date_trunc(date(timestamp_millis(server_time), 'MST'), DAY) = '{ddate}'
        and NET.REG_DOMAIN(page_url) in {domain_list}
),

site_id_to_domain_mapping as
(
    select domain, site_id from
    (
        select
            NET.REG_DOMAIN(page_url) AS domain,
            site_id,
            count(*) requests
        from auction_end_raw
        group by 1, 2
    )
    qualify row_number() over(partition by domain, site_id order by requests desc) = 1
),

page_hits_cte as
(
    select distinct 
        NET.REG_DOMAIN(page_url) AS domain,
        test_name_str,
        test_group,
        session_id
    from pagehits_raw
    qualify count(distinct test_group) over(partition by test_name_str, session_id) = 1
),

auction_end_raw__test as (
    select
        NET.REG_DOMAIN(page_url) AS domain,
        test_name_str,
        test_group,
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
    group by 1, 2, 3, 4
    qualify count(distinct test_group) over(partition by test_name_str, session_id) = 1
),

auction_start_raw__test as (
    select
        NET.REG_DOMAIN(page_url) AS domain,
        test_name_str,
        test_group,
        session_id,
        count(*) as asr_requests
    from auction_start_raw
    group by 1, 2, 3, 4
    qualify count(distinct test_group) over(partition by test_name_str, session_id) = 1
),

-- prebid only tests
bwr_tests as (
    select
        NET.REG_DOMAIN(page_url) AS domain,
        test_name_str,
        test_group,
        session_id,
        sum(cpm / 1e7) as bwr_revenue,
        sum(if(is_native_render, cpm / 1e7, 0)) as bwr_native_render_revenue,
        sum(if(is_gam_bypass, cpm / 1e7, 0)) as bwr_gam_bypass_revenue,
        count(*) as bwr_impressions,
        countif(is_native_render) bwr_native_render_impressions,
        countif(is_gam_bypass) bwr_gam_bypass_impressions
    from bidswon_raw
    group by 1, 2, 3, 4
),

prebid__cte as (
    select *
    from page_hits_cte
    left join auction_start_raw__test using (domain, test_name_str, test_group, session_id)
    left join auction_end_raw__test using (domain, test_name_str, test_group, session_id)
    left join bwr_tests using (domain, test_name_str, test_group, session_id)
),

-- US GAM tests only (for A9/amazon, AdX, EBDA requests only) using dtf
us_gam_dtf as (

    select
        AdUnitId as adunit_id,
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
        and lineitemtype = 'HOUSE'
    group by 1, 2

    union all

    select
        AdUnitId as adunit_id,
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
        and (LineItemID = 0)
    group by 1, 2

    union all

    select
        AdUnitId as adunit_id,
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
    group by 1, 2

    union all

    select
        AdUnitId as adunit_id,
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
    group by 1, 2

    union all

    select
        AdUnitId as adunit_id,
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
        dm.domain,
        aer.test_name_str,
        aer.test_group,
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
    left join `freestar-prod.data_transfer.match_ad_unit_15184186` a
        on a.Id = m.adunit_id and a.date = '{ddate}'
    left join `freestar-prod.NDR_resources.gam_ad_units_map` am
        on am.ad_unit_name = (case when a.Name like '%jcpenney%' then 'jcpenney' else a.Name end)
    join auction_end_raw__test aer
        on aer.session_id = m.session_id
    join site_id_to_domain_mapping dm
        on am.site_id = dm.site_id
    group by 1, 2, 3, 4
),

joined_session_data as (
    select *,
        case
            when bwr_impressions is not null then 'bwr_avail'
            when gam_NBF_impressions is not null then 'GAM_avail'
            when aer_requests is not null then  'aer_avail'
            when asr_requests is not null then 'asr_avail'
            when (asr_requests is NULL) AND (aer_requests is NULL) AND (bwr_impressions is NULL) AND (gam_NBF_impressions is NULL) then 'nothing_avail'
            else 'unknown'
            end as data_status
    from
    prebid__cte
    full outer join
    us_gam_dtf_cte
    using (domain, test_name_str, test_group, session_id)
),

full_session_data as (
    select *,

    CASE data_status
        WHEN 'bwr_avail' THEN coalesce(bwr_revenue, 0) + coalesce(gam_A9_revenue, 0) + coalesce(gam_NBF_revenue, 0)
        WHEN 'GAM_avail' THEN coalesce(gam_prebid_revenue, 0) + coalesce(gam_A9_revenue, 0) + coalesce(gam_NBF_revenue, 0)
        ELSE 0
        END as revenue,

    CASE data_status
        WHEN 'bwr_avail' THEN coalesce(bwr_revenue, 0)
        WHEN 'GAM_avail' THEN coalesce(gam_prebid_revenue, 0)
        ELSE 0
        END as prebid_revenue,

    CASE data_status
        WHEN 'bwr_avail' THEN coalesce(bwr_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) - coalesce(gam_house_impressions, 0)
        WHEN 'GAM_avail' THEN coalesce(gam_prebid_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) - coalesce(gam_house_impressions, 0)
        WHEN 'aer_avail' THEN coalesce(aer_requests, 0) - coalesce(aer_unfilled, 0)
        ELSE 0
        END as impressions,

    CASE data_status
        WHEN 'bwr_avail' THEN coalesce(bwr_impressions, 0)
        WHEN 'GAM_avail' THEN coalesce(gam_prebid_impressions, 0)
        WHEN 'aer_avail' THEN coalesce(aer_requests, 0) - coalesce(aer_unfilled, 0)
        ELSE 0
        END as prebid_impressions,

    CASE data_status
        WHEN 'bwr_avail' THEN coalesce(aer_unfilled, 0) + coalesce(gam_house_impressions, 0)
        WHEN 'GAM_avail' THEN coalesce(gam_LIID0_unfilled, 0) + coalesce(gam_house_impressions, 0)
        WHEN 'aer_avail' THEN coalesce(aer_unfilled, 0)
        WHEN 'asr_avail' THEN coalesce(asr_requests, 0)
        ELSE 0
        END as unfilled,

    CASE data_status
        WHEN 'bwr_avail' THEN coalesce(bwr_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) + coalesce(aer_unfilled, 0)
        WHEN 'GAM_avail' THEN coalesce(gam_prebid_impressions, 0) + coalesce(gam_A9_impressions, 0) + coalesce(gam_NBF_impressions, 0) + coalesce(gam_LIID0_unfilled, 0)
        WHEN 'aer_avail' THEN coalesce(aer_requests, 0)
        WHEN 'asr_avail' THEN coalesce(asr_requests, 0)
        ELSE 0
        END as requests,

    CASE data_status
        WHEN 'unknown' THEN 1
        ELSE 0
        END as unknown_data_status,

    from joined_session_data
)

select '{ddate}' date, domain, test_name_str, test_group,
    count(*) sessions,
    sum(revenue) revenue,
    sum(prebid_revenue) prebid_revenue,
    sum(impressions) impressions,
    sum(prebid_impressions) prebid_impressions,
    sum(unfilled) unfilled,
    sum(requests) requests,
    sum(unknown_data_status) unknown_data_status,
    sum(coalesce(gam_A9_revenue, 0)) gam_amazon_A9_revenue,
    sum(coalesce(gam_NBF_revenue, 0)) gam_NBF_revenue,
    sum(coalesce(gam_A9_impressions, 0)) gam_amazon_A9_impressions,
    sum(coalesce(gam_NBF_impressions, 0)) gam_NBF_impressions,
    sum(coalesce(bwr_native_render_revenue, 0)) bwr_native_render_revenue,
    sum(coalesce(bwr_native_render_impressions, 0)) bwr_native_render_impressions,
    sum(coalesce(gam_house_impressions, 0)) gam_house_impressions

from full_session_data
group by 1, 2, 3, 4;

{create_or_insert_statement}

with domain_test_sessions as
(
    select date, domain, test_name_str, sum(sessions) sessions
    from `streamamp-qa-239417.DAS_increment.BI_AB_raw_page_hits_{name}_{ddate}`
    group by 1, 2, 3
),

domain_primary_test as
(
    select date, domain, test_name_str
    from domain_test_sessions
    qualify sessions = max(sessions) over(partition by domain, date)
)

select *
from `streamamp-qa-239417.DAS_increment.BI_AB_raw_page_hits_{name}_{ddate}`
join domain_primary_test using (date, domain, test_name_str);

drop table `streamamp-qa-239417.DAS_increment.BI_AB_raw_page_hits_{name}_{ddate}`;