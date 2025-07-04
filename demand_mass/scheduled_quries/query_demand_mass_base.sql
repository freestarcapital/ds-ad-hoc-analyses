DECLARE ddate DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- create or replace table sublime-elixir-273810.demand_mass.demand_mass_base_data
-- PARTITION BY date
-- OPTIONS (
-- partition_expiration_days = 365,
-- require_partition_filter = TRUE) AS

BEGIN TRANSACTION;
DELETE FROM `sublime-elixir-273810.demand_mass.demand_mass_base_data`
WHERE date = ddate; --OR date = DATE_SUB(ddate, INTERVAL 1 DAY);
INSERT INTO `sublime-elixir-273810.demand_mass.demand_mass_base_data`
(


WITH device_class_cte AS (
    SELECT
        session_id,
        min(device_class) device_class,
        min(os) os
    FROM
        `freestar-157323.prod_eventstream.pagehits_raw`
    WHERE
    _PARTITIONDATE = ddate
        -- _PARTITIONDATE BETWEEN DATE_SUB(ddate, INTERVAL 1 DAY) AND ddate
    GROUP BY
        session_id
),

auc_end AS (
    SELECT
        DATE(TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), DAY)) AS date,
        placement_id,
        iso AS country_code,
        NET.REG_DOMAIN(auc_end.page_url) AS domain,
        is_empty,auc_end.is_native_render,
        (SELECT REGEXP_EXTRACT(kvps, "fs_auction_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_auction_id=%" LIMIT 1) AS fs_auction_id,
        -- (SELECT REGEXP_EXTRACT(kvps, "fs_clientservermask=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_clientservermask=%" LIMIT 1) AS fs_clientservermask,
        -- (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
        (SELECT REGEXP_EXTRACT(kvps, "fsrefresh=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fsrefresh=%" LIMIT 1) AS fsrefresh,
        -- (SELECT REGEXP_EXTRACT(kvps, "floors_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%floors_id=%" LIMIT 1) AS floors_id,
        (SELECT REGEXP_EXTRACT(kvps, "floors_rtt=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%floors_rtt=%" LIMIT 1) AS floors_rtt,
        (SELECT REGEXP_EXTRACT(kvps, "fs_session_id=(.*)") FROM UNNEST(auc_end.kvps) kvps WHERE kvps LIKE "%fs_session_id=%" LIMIT 1) AS fs_session_id
    FROM
        `freestar-157323.prod_eventstream.auction_end_raw` auc_end
    WHERE
        -- is_native_render and
        -- is_empty
        -- _PARTITIONDATE IN UNNEST(dates)
        -- DATE(TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), DAY)) = ddate
        _PARTITIONDATE = ddate
        -- DATE(TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(server_time), DAY)) BETWEEN DATE_SUB(ddate, INTERVAL 1 DAY) AND ddate
        AND (
            SELECT COUNT(1)
            FROM UNNEST(auc_end.kvps) kvpss
            WHERE
                kvpss LIKE "fs_auction_id=%"
                -- OR kvpss LIKE "fs_testgroup=%"
                -- OR kvpss LIKE "fs_clientservermask=%"
                OR kvpss LIKE "fsrefresh=%"
                OR kvpss LIKE "fs_session_id=%"
                OR kvpss LIKE "floors_rtt=%"
        ) >= 4
),

-- select * from auc_end



agg AS (
    SELECT
        auc_end.date,
        auc_end.country_code,
        `freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os) AS device_category,
        auc_end.domain,
        -- case when floors_id not in ("learning","timeout") then 'optimised' else floors_id end floors_id,
        bidder demand_partner,
        auc_end.fs_auction_id,
        -- auc_end.fs_session_id,
        fs_ad_product,
        `freestar-157323.ad_manager_dtf`.RTTClassify(`freestar-157323.ad_manager_dtf`.device_category_eventstream(device_class, os), CAST(auc_end.floors_rtt AS int64)) AS rtt_category,
        MIN(`freestar-157323.ad_manager_dtf`.FSRefreshClassify(auc_end.fsrefresh)) AS fsrefresh,
        CAST(FORMAT('%.10f', COALESCE(ROUND(SUM(bwr.cpm), 0), 0) / 1e7) AS float64) AS gross_revenue,
        COUNT(DISTINCT CONCAT(auc_end.placement_id, auc_end.fs_auction_id)) AS impressions,
        SUM(IF(is_empty IS TRUE and auc_end.is_native_render is FALSE, 1, 0)) AS unfilled,
        -- count(distinct fs_session_id) session_count
    FROM
        auc_end
    JOIN
        `freestar-157323.prod_eventstream.bidswon_raw` bwr
    ON
        bwr.auction_id = auc_end.fs_auction_id
        AND bwr.placement_id = auc_end.placement_id
    -- AND bwr._PARTITIONDATE BETWEEN DATE_SUB(ddate, INTERVAL 1 DAY) AND ddate
    AND bwr._PARTITIONDATE = ddate
    LEFT JOIN
        device_class_cte
    ON
        auc_end.fs_session_id = device_class_cte.session_id
    WHERE
        auc_end.fsrefresh != 'undefined'
        -- AND fsrefresh<>'' --AND fs_auction_id<>''
    GROUP BY
        1, 2, 3, 4,5,6,7,8
),

  dtf AS (
  SELECT DATE(EventTimestamp) date,
  GeoLookup.CountryCode country_code,
  device_category,
  net.reg_domain(RefererURL) domain,
	-- case when floors_id not in ("learning","timeout") then 'optimised' else floors_id end floors_id,
	CASE WHEN Product in ('AdSense','Ad Exchange') THEN 'Google' WHEN Product = 'Exchange Bidding' THEN 'Open Bidding' ELSE product END demand_partner,
	REGEXP_EXTRACT(CustomTargeting,".*fs-auuid=(.*?)[;$]") fs_auction_id,
	REGEXP_EXTRACT(CustomTargeting,".*fs_ad_product=(.*?)[;$]") fs_ad_product,
  `freestar-157323.ad_manager_dtf`.RTTClassify(device_category, rtt) rtt_category,
  `freestar-157323.ad_manager_dtf`.FSRefreshClassify(fsrefresh) fsrefresh,
  sum(EstimatedBackfillRevenue) AS revenue,
  sum(1) impression,
  sum(0) unfilled
  FROM `freestar-prod.data_transfer.NetworkBackfillImpressions` NetworkImpressions
  LEFT JOIN `freestar-157323.ad_manager_dtf.p_MatchTableGeoTarget_15184186` GeoLookup
  ON GeoLookup.Id = NetworkImpressions.CountryId AND GeoLookup._PARTITIONDATE = EventDateMST
  WHERE fs_clientservermask IS NOT NULL
  AND fs_session_id IS NOT NULL
  AND NetworkImpressions.EventDateMST = ddate
  group by 1,2,3,4,5,6,7,8, 9
  )


select * from agg
union all
select * from dtf
where fs_auction_id is not null


);
COMMIT TRANSACTION;