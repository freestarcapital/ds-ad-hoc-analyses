create or replace table `streamamp-qa-239417.DAS_increment.bidder_impact_raw_{name}_{ddate}` as

WITH device_class_cte AS (
   SELECT
       session_id,
       min(device_class) device_class,
       min(os) os
   FROM
       `freestar-157323.prod_eventstream.pagehits_raw`
   WHERE
       _PARTITIONDATE = '{ddate}'
   GROUP BY
       session_id
),

auc_end AS (
   SELECT
       placement_id,
       DATE(TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), DAY)) AS date,
       iso AS country_code,
       NET.REG_DOMAIN(auc_end.page_url) AS domain,
       session_id,
       fs_auction_id,
       unfilled,
       coalesce(test_name, 'null') test_name_str,
		test_group,
       (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_clientservermask=%" LIMIT 1) AS  fs_clientservermask--,
       --(SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup
   FROM
       `freestar-157323.prod_eventstream.auction_end_raw` auc_end
   WHERE
       _PARTITIONDATE = '{ddate}'
       and NET.REG_DOMAIN(auc_end.page_url) in {domain_list}
       and iso is not null AND TRIM(iso) != ''
       AND (
           SELECT COUNT(1)
           FROM UNNEST(auc_end.kvps) kvpss
           WHERE
               --kvpss LIKE "fs_testgroup=%"
               --OR
               kvpss LIKE "fs_clientservermask=%"
       ) >= 1 --2
   and fs_auction_id is not null
   --and auction_type != 'GAM'
),

auc_end_w_bwr AS (
   SELECT
       auc_end.date,
       auc_end.country_code,
       `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) AS device_category,
       auc_end.domain,
       auc_end.fs_clientservermask,
       --auc_end.fs_testgroup,
       auc_end.session_id,
       auc_end.fs_auction_id,
       auc_end.placement_id,
       bwr.bidder winning_bidder,
       auc_end.test_name_str,
		auc_end.test_group,
       case when unfilled then 0 else 1 end impression,
       case when unfilled then 1 else 0 end unfilled,
       CAST(FORMAT('%.10f', COALESCE(ROUND((bwr.cpm), 0), 0) / 1e7) AS float64) AS revenue,

   FROM
       auc_end
   LEFT JOIN  `freestar-157323.prod_eventstream.bidswon_raw` bwr
   ON
       bwr.fs_auction_id = auc_end.fs_auction_id
       AND bwr.placement_id = auc_end.placement_id
       AND bwr._PARTITIONDATE = '{ddate}'
   LEFT JOIN
       device_class_cte
   ON
       auc_end.session_id = device_class_cte.session_id
   --WHERE fs_testgroup = 'experiment'
)

select * from auc_end_w_bwr
where date = '{ddate}';


create or replace table `streamamp-qa-239417.DAS_increment.bidder_impact_raw_expanded_{name}_{ddate}` as

with expanded AS (
       SELECT offset+1 bidder_position, * except (arr, offset)
       FROM (
                SELECT SPLIT(fs_clientservermask, '') as arr, *
                FROM `streamamp-qa-239417.DAS_increment.bidder_impact_raw_{name}_{ddate}`
            ) AS mask, mask.arr AS mask_value WITH OFFSET AS offset
),

bidder_raw_data as (
    select date, domain, country_code, device_category,
        session_id, fs_auction_id, placement_id,
        test_name_str, test_group,
        winning_bidder, bidder, impression, unfilled, revenue
    from expanded
    LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_bidders` bidders ON bidders.position = expanded.bidder_position
    LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_mask` mask_lookup ON mask_lookup.mask_value = expanded.mask_value
    where status in ('client', 'server')
        and bidder not in ('amazon', 'preGAMAuction')

    union all

    select date, domain, country_code, device_category,
        session_id, fs_auction_id, placement_id,
        test_name_str, test_group,
        winning_bidder, 'ttd' bidder, impression, unfilled, revenue
    FROM `streamamp-qa-239417.DAS_increment.bidder_impact_raw_{name}_{ddate}`
),

brr as (
    select fs_auction_id, placement_id, bidder, bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    WHERE _PARTITIONDATE = '{ddate}'
        and status_message = 'Bid available' and source in ('client', 'server')
)

select brd.*, brr.bidder is not null as bidder_responded, coalesce(brr.bid_cpm, 0) bid_cpm
from bidder_raw_data brd
left join brr using (fs_auction_id, placement_id, bidder)
where date = '{ddate}';


{create_or_insert_statement}

with test_requests as (
    select date, domain, test_name_str, approx_count_distinct(session_id) sessions_day_domain_test
    from `streamamp-qa-239417.DAS_increment.bidder_impact_raw_{name}_{ddate}`
    group by 1, 2, 3
),

domain_primary_test as (
    select *
    from test_requests
    qualify sessions_day_domain_test = max(sessions_day_domain_test) over(partition by date, domain)
),

domain_test_group as (
    select date, domain, test_name_str, test_group,
        count(distinct session_id) sessions_day_domain_test_group,
        sum(revenue) revenue_domain_test_group,
        safe_divide(sum(revenue) , count(distinct session_id)) * 1000 rps_domain_test_group,
        safe_divide(sum(revenue), approx_count_distinct(distinct fs_auction_id || placement_id)) * 1000 cpma_domain_test_group,
        approx_count_distinct(distinct fs_auction_id || placement_id) auctions_domain_test_group,
        count(*) auctions_domain_test_group_2,
        countif(winning_bidder is not null) / count(*) prebid_win_rate_domain_test_group
    from `streamamp-qa-239417.DAS_increment.bidder_impact_raw_{name}_{ddate}`
    join domain_primary_test using (date, domain, test_name_str)
    group by 1, 2, 3, 4
),

t1 as (
    select *,
        least(1, safe_divide(bid_cpm, avg(if(winning_bidder = bidder, bid_cpm, null)) over(partition by fs_auction_id, placement_id))) bid_pressure,
        max(if(winning_bidder is not null, 1, 0)) over(partition by fs_auction_id, placement_id) prebid_wins,
        countif(bidder_responded) over (partition by fs_auction_id, placement_id) >= 1 bidder_response_known,
        count(distinct if(bidder_responded, bidder, null)) over (partition by fs_auction_id, placement_id) count_of_bidder_responses
    from `streamamp-qa-239417.DAS_increment.bidder_impact_raw_expanded_{name}_{ddate}`
    join domain_test_group using (date, domain, test_name_str, test_group)
),

results as (
    select domain, date, bidder, test_name_str, test_group,
        safe_divide(countif(bidder_responded and bidder_response_known), countif(bidder_response_known)) bidder_participation_rate,
        safe_divide(countif((winning_bidder is not null) and (winning_bidder = bidder)), count(*)) bidder_win_rate,
        safe_divide(countif((winning_bidder is not null) and (winning_bidder = bidder)), countif(winning_bidder is not null)) bidder_prebid_win_rate,
        avg(if(bidder_response_known, count_of_bidder_responses, null)) count_of_bidder_responses,
        avg(if(bidder_response_known and bidder_responded, bid_cpm, null)) bidder_cpm_when_bids,
        avg(if(bidder_response_known and bidder_responded and (winning_bidder = bidder), bid_cpm, null)) bidder_cpm_when_wins,
        avg(if((winning_bidder is not null) and bidder_response_known, bid_pressure, null)) bidder_price_pressure_include_non_bids,
        avg(if((winning_bidder is not null) and bidder_responded and bidder_response_known, bid_pressure, null)) bidder_price_pressure_bids,
        avg(if((winning_bidder is not null) and bidder_responded and bidder_response_known and bidder = winning_bidder, bid_pressure, null)) bidder_price_pressure_wins,
        avg(if(winning_bidder is not null and bidder_response_known, if(bid_pressure >= 0.8, 1, 0), null)) bidder_within_20perc_include_non_bids,
        avg(if((winning_bidder is not null) and bidder_responded and bidder_response_known, if(bid_pressure >= 0.8, 1, 0), null)) bidder_within_20perc_bids,
        avg(if(winning_bidder is not null and bidder_response_known, if(bid_pressure >= 0.5, 1, 0), null)) bidder_within_50perc_include_non_bids,
        avg(if((winning_bidder is not null) and bidder_responded and bidder_response_known, if(bid_pressure >= 0.5, 1, 0), null)) bidder_within_50perc_bids,
        avg(sessions_day_domain_test_group) sessions_day_domain_test_group,
        avg(revenue_domain_test_group) revenue_domain_test_group,
        avg(rps_domain_test_group) rps_domain_test_group,
        avg(prebid_win_rate_domain_test_group) prebid_win_rate_domain_test_group,
        avg(cpma_domain_test_group) cpma_domain_test_group,
        avg(auctions_domain_test_group_2) auctions_domain_test_group_2,
        avg(prebid_wins) prebid_wins,
        avg(auctions_domain_test_group) auctions_domain_test_group
    from t1
    group by 1, 2, 3, 4, 5
)

select '{name}' ab_test_name, *,
    (countif(sessions >= {minimum_sessions}) over (partition by date, domain, test_name_str) = 2) and (test_name_str != 'null') test_running
from results;

drop table `streamamp-qa-239417.DAS_increment.bidder_impact_raw_{name}_{ddate}`;
drop table `streamamp-qa-239417.DAS_increment.bidder_impact_raw_expanded_{name}_{ddate}`;
