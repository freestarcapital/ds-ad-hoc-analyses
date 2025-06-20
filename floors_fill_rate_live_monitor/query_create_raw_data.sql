DECLARE window_days int64 DEFAULT 20;

DECLARE midnight_UTC_after_last_date TIMESTAMP DEFAULT TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY);
DECLARE last_date_dow_offset int64 DEFAULT(EXTRACT(DAYOFWEEK FROM midnight_UTC_after_last_date) - 1);

CREATE OR REPLACE TABLE `{table_name}`
(
    time TIMESTAMP,
    time_DAY TIMESTAMP,
    time_WEEK TIMESTAMP,
    device_category STRING,
    country_code STRING,
    ad_unit_name STRING,
    upr_id STRING,
    revenue NUMERIC,
    programmatic_impressions NUMERIC,
    ad_requests NUMERIC,
    floor_price NUMERIC
 )
 OPTIONS(
   expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
);

INSERT INTO `{table_name}`
with t1 as (
    SELECT
        TIMESTAMP((extract(datetime from date_hour at time zone c.time_zone))) time,
        device_category, reporting.country_code, LOWER(ad_unit_name) ad_unit_name, floors_id upr_id,
        SUM(IFNULL(revenue, 0)) revenue,
        SUM(IFNULL(CASE WHEN advertiser="House" THEN 0 ELSE impressions END, 0)) programmatic_impressions,
        SUM(IFNULL(impressions, 0)) + SUM(IFNULL(unfilled, 0)) AS ad_requests
    FROM `sublime-elixir-273810.floors.detailed_reporting` reporting
    JOIN `sublime-elixir-273810.floors.group_ad_manager_lookup` look
    ON look.ad_manager_id = reporting.ad_manager_id
    join `sublime-elixir-273810.ds_country_eu.time_zone` c on reporting.country_code = c.country_code
    WHERE TIMESTAMP_SUB(midnight_UTC_after_last_date, INTERVAL window_days+1 DAY) <= date_hour and date_hour < TIMESTAMP_ADD(midnight_UTC_after_last_date, INTERVAL 1 DAY)
    AND ((`sublime-elixir-273810.floors`.IsDynamic(advertiser) AND IFNULL(SAFE_DIVIDE(revenue, IFNULL(impressions, 0))*1000, 100) <= 30)
        OR (IFNULL(unfilled, 0) > 0))
    AND (revenue IS NULL OR revenue >= 0)
    AND (impressions IS NULL OR impressions >= 0)
    GROUP BY time, device_category, reporting.country_code, ad_unit_name, upr_id
), t2 as
(
    select
        time,
        TIMESTAMP_TRUNC(time, DAY) time_DAY,
        TIMESTAMP_ADD(TIMESTAMP_TRUNC(TIMESTAMP_SUB(time, INTERVAL last_date_dow_offset DAY), WEEK), INTERVAL last_date_dow_offset DAY) time_WEEK,
        device_category, country_code, ad_unit_name, upr_id,
        revenue, programmatic_impressions, ad_requests,
        IFNULL((SELECT floor_price FROM `sublime-elixir-273810.floors.upr_map` map WHERE map.upr_id = t1.upr_id), 0) floor_price
    from t1
    WHERE TIMESTAMP_SUB(midnight_UTC_after_last_date, INTERVAL window_days DAY) <= time and time < midnight_UTC_after_last_date
)
select time, time_DAY, time_WEEK,
    device_category, country_code, ad_unit_name, upr_id,
    revenue, programmatic_impressions, ad_requests, floor_price
from t2;

