
DECLARE dates ARRAY<DATE> DEFAULT GENERATE_DATE_ARRAY(DATE_SUB('{processing_date}', INTERVAL {days_back_start} DAY), DATE_SUB('{processing_date}', INTERVAL {days_back_end} DAY));

CREATE OR REPLACE TABLE `{project_id}.DAS_eventstream_session_data.eventstream_DAS_expt_stats_split_revenue_{processing_date}_{days_back_start}_{days_back_end}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

WITH device_class_cte AS (
    SELECT
        session_id,
        min(device_class) device_class,
        min(os) os
    FROM
        `freestar-157323.prod_eventstream.pagehits_raw`
    WHERE
        _PARTITIONDATE IN UNNEST(dates)
    GROUP BY
        session_id
),

auc_end AS (
    SELECT
        server_time,
        placement_id,
        DATE(TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), DAY)) AS date,
        iso AS country_code,
        NET.REG_DOMAIN(auc_end.page_url) AS domain,
        --is_empty, is_native_render,
        (SELECT REGEXP_EXTRACT(kvps, "fs_auction_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_auction_id=%" LIMIT 1) AS fs_auction_id,
        (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_clientservermask=%" LIMIT 1) AS  fs_clientservermask,
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
        -- not sure refresh is right in this file, so removing for now - min(.) later on worries me
        --(SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fsrefresh=%" LIMIT 1) AS fsrefresh,
        -- (SELECT REGEXP_EXTRACT(kvps, "floors_rtt=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%floors_rtt=%" LIMIT 1) AS floors_rtt,
        (SELECT REGEXP_EXTRACT(kvps, "fs_session_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_session_id=%" LIMIT 1) AS fs_session_id
    FROM
        `freestar-157323.prod_eventstream.auction_end_raw` auc_end
    WHERE
        -- is_native_render and
        -- is_empty
        _PARTITIONDATE IN UNNEST(dates)
        AND (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE
                kvpss LIKE "fs_auction_id=%"
                OR kvpss LIKE "fs_testgroup=%"
                OR kvpss LIKE "fs_clientservermask=%"
                --OR kvpss LIKE "fsrefresh=%"
                OR kvpss LIKE "fs_session_id=%"
                -- OR kvpss LIKE "floors_rtt=%"
        ) >= 4
),

session_agg AS (
    SELECT
        auc_end.country_code,
        --auc_end.domain,
        auc_end.fs_clientservermask,
        auc_end.fs_session_id,
        -- `freestar-157323.ad_manager_dtf`.RTTClassify(`freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os), CAST(auc_end.floors_rtt AS int64)) AS rtt_category,
        `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) AS device_category,
        --MIN(`freestar-157323.ad_manager_dtf`.FSRefreshClassify(auc_end.fsrefresh)) AS fsrefresh, -- not convinced this is right - should it really be min?
        CAST(FORMAT('%.10f', COALESCE(ROUND(SUM(bwr.cpm), 0), 0) / 1e7) AS float64) AS revenue,
        min(auc_end.date) date,
        --COUNT(DISTINCT CONCAT(auc_end.placement_id, auc_end.fs_auction_id)) AS impressions,
        -- SUM(IF(is_empty IS TRUE and is_native_render is FALSE, 1, 0)) AS unfilled,
        --count(distinct fs_session_id) session_count
    FROM
        auc_end
    JOIN
        `freestar-157323.prod_eventstream.bidswon_raw` bwr
    ON
        bwr.auction_id = auc_end.fs_auction_id
        AND bwr.placement_id = auc_end.placement_id
    AND bwr._PARTITIONDATE IN UNNEST(dates)
    LEFT JOIN
        device_class_cte
    ON
        auc_end.fs_session_id = device_class_cte.session_id
    WHERE
        --auc_end.fsrefresh != 'undefined'
        -- AND fsrefresh<>'' --AND fs_auction_id<>''
        fs_testgroup='experiment'
        and fs_clientservermask is not null
    GROUP BY
        1, 2, 3, 4--, 5--, 6
)

select country_code, fs_clientservermask, fs_session_id, device_category, date, revenue
    --sum(revenue) over(partition by fs_session_id) revenue
    from session_agg
