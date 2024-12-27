CREATE OR REPLACE TABLE `streamamp-qa-239417.Floors_2_0.floors_ad_unit_base`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

select date_hour,
    ad_unit_name,
    floor_price,
    coalesce(impressions, 0) + coalesce(unfilled, 0) requests,
    coalesce(if(advertiser="House" or advertiser="Internal", 0, impressions), 0) impressions,
    CAST(coalesce(revenue, 0) as FLOAT64) revenue,
    floors_id != 'control' and floors_id != 'learning' optimised,
    floors_id = 'control' baseline
from `sublime-elixir-273810.floors.detailed_reporting`
left JOIN `sublime-elixir-273810.floors.upr_map`
    ON floors_id = upr_id
where date_hour >= '{first_date}' and date_hour <= '{last_date}'
    and floor_price is not null
