
CREATE OR REPLACE TABLE `streamamp-qa-239417.Floors_2_0.floors_ad_unit_dash`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with base as (
    select date_hour,
        ad_unit_name,
        floor_price,
        floor_price is not null floor_price_valid,
        coalesce(impressions, 0) + coalesce(unfilled, 0) requests,
        coalesce(if(advertiser="House" or advertiser="Internal", 0, impressions), 0) impressions,
        CAST(coalesce(revenue, 0) as FLOAT64) revenue,
        floors_id != 'control' and floors_id != 'learning' optimised,
        floors_id = 'control' baseline
    from `sublime-elixir-273810.floors.detailed_reporting`
    left JOIN `sublime-elixir-273810.floors.upr_map`
        ON floors_id = upr_id
    where date_hour >= TIMESTAMP(DATE_SUB('{first_date}', INTERVAL 3 DAY))
        and date_hour <= '{last_date}'
),

hourly_analytics as (

    select date_hour, ad_unit_name,

        sum(requests) requests,
        sum(impressions) impressions,
        sum(revenue) revenue,

        COALESCE(SAFE_DIVIDE(SUM(if(optimised, impressions, 0)), SUM(if(optimised, requests, 0))), 0) optimised_fill_rate,
        COALESCE(SAFE_DIVIDE(SUM(if(optimised, revenue, 0)), SUM(if(optimised, impressions, 0))), 0) * 1000 optimised_cpm_,
        COALESCE(SAFE_DIVIDE(SUM(if(optimised, revenue, 0)), SUM(if(optimised, requests, 0))), 0) * 1000 optimised_cpma,
        
        COALESCE(SAFE_DIVIDE(SUM(if(baseline, impressions, 0)), SUM(if(baseline, requests, 0))), 0) baseline_fill_rate,
        COALESCE(SAFE_DIVIDE(SUM(if(baseline, revenue, 0)), SUM(if(baseline, impressions, 0))), 0) * 1000 baseline_cpm_,
        COALESCE(SAFE_DIVIDE(SUM(if(baseline, revenue, 0)), SUM(if(baseline, requests, 0))), 0) * 1000 baseline_cpma,

        (safe_divide(COALESCE(SAFE_DIVIDE(SUM(if(baseline, revenue, 0)), SUM(if(baseline, requests, 0))), 0),
            COALESCE(SAFE_DIVIDE(SUM(if(optimised, revenue, 0)), SUM(if(optimised, requests, 0))), 0)) - 1) * 100 price__pressure,

        CAST(safe_divide(sum(if(floor_price_valid, floor_price * requests, 0)), sum(if(floor_price_valid, requests, 0))) AS FLOAT64) floor_price

    from base    
    group by 1, 2
),

stats as (
    select *,
        avg(requests) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) requests_sm,
        avg(impressions) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) impressions_sm,
        avg(revenue) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) revenue_sm,
        avg(optimised_fill_rate) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) optimised_fill_rate_sm,
        avg(baseline_fill_rate) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) baseline_fill_rate_sm,
        avg(optimised_cpm_) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) optimised_cpm_sm,
        avg(baseline_cpm_) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) baseline_cpm_sm,
        avg(optimised_cpma) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) optimised_cpma_sm,
        avg(baseline_cpma) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) baseline_cpma_sm,
        avg(price__pressure) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) price_pressure_sm,
        avg(floor_price) over(order by date_hour, ad_unit_name rows between {N} preceding and current row) floor_price_sm

    from hourly_analytics
)

select *
from stats
where date_hour >= '{first_date}' and date_hour <= '{last_date}'
order by date_hour
