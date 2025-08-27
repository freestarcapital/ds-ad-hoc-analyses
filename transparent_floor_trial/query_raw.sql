DECLARE ddate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

create or replace table `streamamp-qa-239417.DAS_increment.transparent_raw` as

WITH device_class_cte AS (
    SELECT
        session_id,
        min(device_class) device_class,
        min(os) os
    FROM
        `freestar-157323.prod_eventstream.pagehits_raw`
    WHERE
        _PARTITIONDATE = ddate
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
        test_name,
		test_group,
        (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_clientservermask=%" LIMIT 1) AS  fs_clientservermask,
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup
    FROM
        `freestar-157323.prod_eventstream.auction_end_raw` auc_end
    WHERE
        _PARTITIONDATE = ddate
        and NET.REG_DOMAIN(auc_end.page_url) = 'pro-football-reference.com'
        and iso is not null AND TRIM(iso) != ''
        AND (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE
                kvpss LIKE "fs_testgroup=%"
                OR kvpss LIKE "fs_clientservermask=%"
        ) >= 2
    and fs_auction_id is not null
    and auction_type != 'GAM'
),

auc_end_w_bwr AS (
    SELECT
        auc_end.date,
        auc_end.country_code,
        `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) AS device_category,
        auc_end.domain,
        auc_end.fs_clientservermask,
        auc_end.fs_testgroup,
        auc_end.session_id,
        auc_end.fs_auction_id,
        auc_end.placement_id,
        bwr.bidder winning_bidder,
        auc_end.test_name,
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
        AND bwr._PARTITIONDATE = ddate
    LEFT JOIN
        device_class_cte
    ON
        auc_end.session_id = device_class_cte.session_id
    WHERE fs_testgroup='experiment'
)

select * from auc_end_w_bwr;



DECLARE ddate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

create or replace table `streamamp-qa-239417.DAS_increment.transparent_raw_expanded` as

with expanded AS (
       SELECT offset+1 bidder_position, * except (arr, offset)
       FROM (
                SELECT SPLIT(fs_clientservermask, '') as arr, *
                FROM `streamamp-qa-239417.DAS_increment.transparent_raw`
            ) AS mask, mask.arr AS mask_value WITH OFFSET AS offset
),

bidder_raw_data as (
    select date, domain, country_code, device_category,
        session_id, fs_auction_id, placement_id,
        test_name, test_group,
        winning_bidder, bidder, impression, unfilled, revenue
    from expanded
    LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_bidders` bidders ON bidders.position = expanded.bidder_position
    LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_mask` mask_lookup ON mask_lookup.mask_value = expanded.mask_value
    where status = 'client'
        and bidder not in ('amazon', 'preGAMAuction')
),

brr as (
    select fs_auction_id, placement_id, bidder, bid_cpm
    from `freestar-157323.prod_eventstream.bidsresponse_raw`
    WHERE _PARTITIONDATE = ddate
        and status_message = 'Bid available' and source = 'client'
)

select brd.*, brr.bidder is not null as bidder_responded, coalesce(brr.bid_cpm, 0) bid_cpm
from bidder_raw_data brd
left join brr using (fs_auction_id, placement_id, bidder);

with t1 as  (
    select test_group, fs_auction_id, placement_id,
        count(*) bidders_called, countif(bidder_responded) responses,
        countif(bidder_responded) / count(*) bidder_participation_rate
    from `streamamp-qa-239417.DAS_increment.transparent_raw_expanded`
    where test_name = 'c4c21675-1f3f-4e6b-910a-9577f128c051'
    group by 1, 2, 3
    having countif(bidder_responded) >= 1
)
select test_group, avg(bidder_participation_rate) bidder_participation_rate
from t1
group by 1;




with t1 as (
select *
from `streamamp-qa-239417.DAS_increment.transparent_raw_expanded`
qualify countif(bidder_responded) over (partition by fs_auction_id, placement_id) >= 1
), t2 as (
select bidder, test_group, countif(bidder_responded)/count(*) bidder_participation_rate
from t1
where test_name = 'c4c21675-1f3f-4e6b-910a-9577f128c051'
group by 1, 2
), t3 as (
select bidder, avg(if(test_group=0, bidder_participation_rate, null)) bidder_participation_rate_test_group_0,
    avg(if(test_group=1, bidder_participation_rate, null)) bidder_participation_rate_test_group_1
from t2
group by 1
)
select *, 100*safe_divide(bidder_participation_rate_test_group_1-bidder_participation_rate_test_group_0, 0.5*(bidder_participation_rate_test_group_0+bidder_participation_rate_test_group_1)) delta_percent from t3
order by 1;



select test_group, approx_count_distinct(session_id) sessions, approx_count_distinct(fs_auction_id) auctions,
    sum(impression) impressions, sum(revenue) revenue
from `streamamp-qa-239417.DAS_increment.transparent_raw`
where test_name = 'c4c21675-1f3f-4e6b-910a-9577f128c051'
group by 1
limit 100