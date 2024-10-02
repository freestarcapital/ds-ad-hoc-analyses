
DECLARE dates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE_SUB('{processing_date}', INTERVAL {days_back_start} DAY), DATE_SUB('{processing_date}', INTERVAL {days_back_end} DAY));

--CREATE OR REPLACE TABLE `{project_id}.DAS_increment.daily_bidder_domain_expt_session_stats_unexpanded_{aer_to_bwr_join_type}_{processing_date}_{days_back_start}_{days_back_end}`
--    OPTIONS (
--        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
--    AS
--
--WITH device_class_cte AS (
--    SELECT
--        session_id,
--        min(device_class) device_class,
--        min(os) os
--    FROM
--        `freestar-157323.prod_eventstream.pagehits_raw`
--    WHERE
--        _PARTITIONDATE IN UNNEST(dates)
--    GROUP BY
--        session_id
--),
--
--auc_end AS (
--    SELECT
--        TIMESTAMP_MILLIS(server_time) ts,
--        placement_id,
--        DATE(TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), DAY)) AS date,
--        iso AS country_code,
--        NET.REG_DOMAIN(auc_end.page_url) AS domain,
--        --is_empty, is_native_render,
--        (SELECT REGEXP_EXTRACT(kvps, "fs_auction_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_auction_id=%" LIMIT 1) AS fs_auction_id,
--        (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_clientservermask=%" LIMIT 1) AS  fs_clientservermask,
--        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
--        -- not sure refresh is right in this file, so removing for now - min(.) later on worries me
--        --(SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fsrefresh=%" LIMIT 1) AS fsrefresh,
--        (SELECT REGEXP_EXTRACT(kvps, "floors_rtt=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%floors_rtt=%" LIMIT 1) AS floors_rtt,
--        (SELECT REGEXP_EXTRACT(kvps, "fs_session_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_session_id=%" LIMIT 1) AS fs_session_id
--    FROM
--        `freestar-157323.prod_eventstream.auction_end_raw` auc_end
--    WHERE
--        -- is_native_render and
--        -- is_empty
--        _PARTITIONDATE IN UNNEST(dates)
--        AND (
--            SELECT COUNT(1)
--            FROM UNNEST(auc_end.kvps) kvpss
--            WHERE
--                kvpss LIKE "fs_auction_id=%"
--                OR kvpss LIKE "fs_testgroup=%"
--                OR kvpss LIKE "fs_clientservermask=%"
--                --OR kvpss LIKE "fsrefresh=%"
--                OR kvpss LIKE "fs_session_id=%"
--                OR kvpss LIKE "floors_rtt=%"
--        ) >= 5
--),
--
--session_agg AS (
--    SELECT
--        auc_end.country_code,
--        auc_end.domain,
--        auc_end.fs_clientservermask,
--        auc_end.fs_session_id,
--        bwr.bidder winning_bidder,
--        `freestar-157323.ad_manager_dtf`.RTTClassify(`freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os), CAST(auc_end.floors_rtt AS int64)) AS rtt_category,
--        `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) AS device_category,
--        --fs_testgroup,
--        --MIN(`freestar-157323.ad_manager_dtf`.FSRefreshClassify(auc_end.fsrefresh)) AS fsrefresh, -- not convinced this is right - should it really be min?
--        CAST(FORMAT('%.10f', COALESCE(ROUND(SUM(bwr.cpm), 0), 0) / 1e7) AS float64) AS revenue,
--        min(auc_end.date) date,
--        min(auc_end.ts) ts
--    FROM
--        auc_end
--    {aer_to_bwr_join_type}
--        `freestar-157323.prod_eventstream.bidswon_raw` bwr
--    ON
--        bwr.auction_id = auc_end.fs_auction_id
--        AND bwr.placement_id = auc_end.placement_id
--    AND bwr._PARTITIONDATE IN UNNEST(dates)
--    LEFT JOIN
--        device_class_cte
--    ON
--        auc_end.fs_session_id = device_class_cte.session_id
--    WHERE
--        --auc_end.fsrefresh != 'undefined'
--        -- AND fsrefresh<>'' --AND fs_auction_id<>''
--        fs_testgroup = 'experiment'
--        --fs_testgroup in ('experiment')
--    GROUP BY
--        1, 2, 3, 4, 5, 6, 7
--)
--select * from session_agg;


--CREATE OR REPLACE TABLE `{project_id}.DAS_increment.daily_bidder_domain_expt_session_stats_{aer_to_bwr_join_type}_{processing_date}_{days_back_start}_{days_back_end}`
--    OPTIONS (
--        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
--    AS
--
--with expanded AS (
--    SELECT offset+1 bidder_position, * except (arr, offset)
--    FROM (
--        SELECT SPLIT(fs_clientservermask, '') as arr, *
--        FROM `{project_id}.DAS_increment.daily_bidder_domain_expt_session_stats_unexpanded_{aer_to_bwr_join_type}_{processing_date}_{days_back_start}_{days_back_end}`
--    ) AS mask, mask.arr AS mask_value WITH OFFSET AS offset
--)
--
--select date, bidder,
--    if((bidder in  ('ix', 'rise', 'appnexus', 'rubicon', 'triplelift', 'pubmatic') and date >= '2024-08-28' and status = 'disabled')
--            or (bidder in  ('yieldmo', 'sharethrough', 'criteo', 'medianet', 'openx', 'gumgum', 'yahoo') and date >= '2024-09-24' and status = 'disabled'),
--        'client', status) status,
--    country_code, domain, device_category, rtt_category,
--    count(*) as session_count,
--    sum(if(bidders.bidder = winning_bidder, revenue, 0)) revenue,
--    sum(if(bidders.bidder = winning_bidder, pow(revenue, 2), 0)) revenue_sq,
--    sum(if(bidders.bidder = winning_bidder, 1, 0)) wins
--
--from expanded
--LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_bidders` bidders ON bidders.position = expanded.bidder_position
--LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_mask` mask_lookup ON mask_lookup.mask_value = expanded.mask_value
--group by 1, 2, 3, 4, 5, 6, 7;


CREATE OR REPLACE TABLE `{project_id}.DAS_increment.daily_bidder_domain_expt_session_stats_cbc_{aer_to_bwr_join_type}_{processing_date}_{days_back_start}_{days_back_end}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with base as (
    select *,
        array_length(REGEXP_EXTRACT_ALL(substr(fs_clientservermask, 2, 21), '2'))
            + if(date >= '2024-08-28', 6, 0) + if(date >= '2024-09-24', 7, 0) AS client_bidders
    FROM `{project_id}.DAS_increment.daily_bidder_domain_expt_session_stats_unexpanded_{aer_to_bwr_join_type}_{processing_date}_{days_back_start}_{days_back_end}`
),

expanded AS (
    SELECT offset+1 bidder_position, * except (arr, offset)
    FROM (
        SELECT SPLIT(fs_clientservermask, '') as arr, *
        FROM base
    ) AS mask, mask.arr AS mask_value WITH OFFSET AS offset
)

select date, bidder,
    if((bidder in ('ix', 'rise', 'appnexus', 'rubicon', 'triplelift', 'pubmatic') and date >= '2024-08-28' and status = 'disabled')
            or (bidder in  ('yieldmo', 'sharethrough', 'criteo', 'medianet', 'openx', 'gumgum', 'yahoo') and date >= '2024-09-24' and status = 'disabled'),
        'client', status) status,
    country_code, domain, device_category, rtt_category, client_bidders,
    count(*) as session_count,
    sum(if(bidders.bidder = winning_bidder, revenue, 0)) revenue,
    sum(if(bidders.bidder = winning_bidder, pow(revenue, 2), 0)) revenue_sq,
    sum(if(bidders.bidder = winning_bidder, 1, 0)) wins

from expanded
LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_bidders` bidders ON bidders.position = expanded.bidder_position
LEFT JOIN `freestar-157323.ad_manager_dtf.lookup_mask` mask_lookup ON mask_lookup.mask_value = expanded.mask_value
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
