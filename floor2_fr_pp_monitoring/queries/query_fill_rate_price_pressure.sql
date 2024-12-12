
with base as (
    select {date_hour},
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
        and {ad_unit_name_match}
),

hourly_analytics as (

    select date_hour,

        SUM(if(optimised, requests, 0)) optimised_requests,
        SUM(if(optimised, impressions, 0)) optimised_impressions,
        SUM(if(optimised, revenue, 0)) optimised_revenue,
        COALESCE(SAFE_DIVIDE(SUM(if(optimised, impressions, 0)), SUM(if(optimised, requests, 0))), 0) optimised_fill_rate,
        COALESCE(SAFE_DIVIDE(SUM(if(optimised, revenue, 0)), SUM(if(optimised, impressions, 0))), 0) * 1000 optimised_cpm_,
        COALESCE(SAFE_DIVIDE(SUM(if(optimised, revenue, 0)), SUM(if(optimised, requests, 0))), 0) * 1000 optimised_cpma,
        
        SUM(if(baseline, requests, 0)) baseline_requests,
        SUM(if(baseline, impressions, 0)) baseline_impressions,
        SUM(if(baseline, revenue, 0)) baseline_revenue,
        COALESCE(SAFE_DIVIDE(SUM(if(baseline, impressions, 0)), SUM(if(baseline, requests, 0))), 0) baseline_fill_rate,
        COALESCE(SAFE_DIVIDE(SUM(if(baseline, revenue, 0)), SUM(if(baseline, impressions, 0))), 0) * 1000 baseline_cpm_,
        COALESCE(SAFE_DIVIDE(SUM(if(baseline, revenue, 0)), SUM(if(baseline, requests, 0))), 0) * 1000 baseline_cpma,

        CAST(safe_divide(sum(if(floor_price_valid, floor_price * requests, 0)), sum(if(floor_price_valid, requests, 0))) AS FLOAT64) floor_price

    from base    
    group by 1
),

stats as (
    select *,
        avg(optimised_requests) over(order by date_hour rows between {N} preceding and current row) {granularity}_optimised_requests_sm,
        sqrt((avg(power(optimised_requests, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(optimised_requests) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_optimised_requests_sm_err,

        avg(baseline_requests) over(order by date_hour rows between {N} preceding and current row) {granularity}_baseline_requests_sm,
        sqrt((avg(power(baseline_requests, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(baseline_requests) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_baseline_requests_sm_err,

        avg(optimised_impressions) over(order by date_hour rows between {N} preceding and current row) {granularity}_optimised_impressions_sm,
        sqrt((avg(power(optimised_impressions, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(optimised_impressions) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_optimised_impressions_sm_err,

        avg(baseline_impressions) over(order by date_hour rows between {N} preceding and current row) {granularity}_baseline_impressions_sm,
        sqrt((avg(power(baseline_impressions, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(baseline_impressions) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_baseline_impressions_sm_err,

        avg(optimised_revenue) over(order by date_hour rows between {N} preceding and current row) {granularity}_optimised_revenue_sm,
        sqrt((avg(power(optimised_revenue, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(optimised_revenue) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_optimised_revenue_sm_err,

        avg(baseline_revenue) over(order by date_hour rows between {N} preceding and current row) {granularity}_baseline_revenue_sm,
        sqrt((avg(power(baseline_revenue, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(baseline_revenue) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) {granularity}_baseline_revenue_sm_err,

        avg(optimised_fill_rate) over(order by date_hour rows between {N} preceding and current row) optimised_fill_rate_sm,
        sqrt((avg(power(optimised_fill_rate, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(optimised_fill_rate) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) optimised_fill_rate_sm_err,

        avg(baseline_fill_rate) over(order by date_hour rows between {N} preceding and current row) baseline_fill_rate_sm,
        sqrt((avg(power(baseline_fill_rate, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(baseline_fill_rate) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) baseline_fill_rate_sm_err,
        
        avg(optimised_cpm_) over(order by date_hour rows between {N} preceding and current row) optimised_cpm_sm,
        sqrt((avg(power(optimised_cpm_, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(optimised_cpm_) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) optimised_cpm_sm_err,

        avg(baseline_cpm_) over(order by date_hour rows between {N} preceding and current row) baseline_cpm_sm,
        sqrt((avg(power(baseline_cpm_, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(baseline_cpm_) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) baseline_cpm_sm_err,

        avg(optimised_cpma) over(order by date_hour rows between {N} preceding and current row) optimised_cpma_sm,
        sqrt((avg(power(optimised_cpma, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(optimised_cpma) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) optimised_cpma_sm_err,

        avg(baseline_cpma) over(order by date_hour rows between {N} preceding and current row) baseline_cpma_sm,
        sqrt((avg(power(baseline_cpma, 2)) over(order by date_hour rows between {N} preceding and current row) -
            power(avg(baseline_cpma) over(order by date_hour rows between {N} preceding and current row), 2))/({N}+1)) baseline_cpma_sm_err,

        safe_divide(avg(optimised_impressions) over(order by date_hour rows between {N} preceding and current row),
            avg(optimised_requests) over(order by date_hour rows between {N} preceding and current row)) optimised_fill_rate_sm_ratio,

        safe_divide(avg(baseline_impressions) over(order by date_hour rows between {N} preceding and current row),
            avg(baseline_requests) over(order by date_hour rows between {N} preceding and current row)) baseline_fill_rate_sm_ratio,

        safe_divide(avg(optimised_revenue) over(order by date_hour rows between {N} preceding and current row),
            avg(optimised_impressions) over(order by date_hour rows between {N} preceding and current row)) * 1000 optimised_cpm_sm_ratio,

        safe_divide(avg(baseline_revenue) over(order by date_hour rows between {N} preceding and current row),
            avg(baseline_impressions) over(order by date_hour rows between {N} preceding and current row)) * 1000 baseline_cpm_sm_ratio,

        safe_divide(avg(optimised_revenue) over(order by date_hour rows between {N} preceding and current row),
            avg(optimised_requests) over(order by date_hour rows between {N} preceding and current row)) * 1000 optimised_cpma_sm_ratio,

        safe_divide(avg(baseline_revenue) over(order by date_hour rows between {N} preceding and current row),
            avg(baseline_requests) over(order by date_hour rows between {N} preceding and current row)) * 1000 baseline_cpma_sm_ratio,

    from hourly_analytics
),

perc_err as (

    select *,
        {granularity}_optimised_requests_sm_err / {granularity}_optimised_requests_sm * 100 perc_optimised_requests_sm,
        {granularity}_optimised_impressions_sm_err / {granularity}_optimised_impressions_sm * 100 perc_optimised_impressions_sm,
        {granularity}_optimised_revenue_sm_err / {granularity}_optimised_revenue_sm * 100 perc_optimised_revenue_sm,
        {granularity}_baseline_requests_sm_err / {granularity}_baseline_requests_sm * 100 perc_baseline_requests_sm,
        {granularity}_baseline_impressions_sm_err / {granularity}_baseline_impressions_sm * 100 perc_baseline_impressions_sm,
        {granularity}_baseline_revenue_sm_err / {granularity}_baseline_revenue_sm * 100 perc_baseline_revenue_sm
        
    from stats
    where date_hour >= '{first_date}' and date_hour <= '{last_date}'

)    
        
select *, 
    sqrt(power(perc_optimised_requests_sm, 2) + power(perc_optimised_impressions_sm, 2)) / 100 * optimised_fill_rate_sm_ratio optimised_fill_rate_sm_ratio_err,
    sqrt(power(perc_optimised_impressions_sm, 2) + power(perc_optimised_revenue_sm, 2)) / 100 * optimised_cpm_sm_ratio optimised_cpm_sm_ratio_err,
    sqrt(power(perc_optimised_requests_sm, 2) + power(perc_optimised_revenue_sm, 2)) / 100 * optimised_cpma_sm_ratio optimised_cpma_sm_ratio_err,
    sqrt(power(perc_baseline_requests_sm, 2) + power(perc_baseline_impressions_sm, 2)) / 100 * baseline_fill_rate_sm_ratio baseline_fill_rate_sm_ratio_err,
    sqrt(power(perc_baseline_impressions_sm, 2) + power(perc_baseline_revenue_sm, 2)) / 100 * baseline_cpm_sm_ratio baseline_cpm_sm_ratio_err,
    sqrt(power(perc_baseline_requests_sm, 2) + power(perc_baseline_revenue_sm, 2)) / 100 * baseline_cpma_sm_ratio baseline_cpma_sm_ratio_err,
    (baseline_cpma_sm / optimised_cpma_sm - 1) * 100 price_pressure_sm,
    (baseline_cpma_sm_ratio / optimised_cpma_sm_ratio - 1) * 100 price_pressure_sm_ratio


from perc_err 
order by date_hour
