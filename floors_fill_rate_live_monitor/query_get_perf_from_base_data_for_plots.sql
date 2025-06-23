
with fill_rate as (
    select time_day as date,
        SUM(revenue) revenue_fr,
        SUM(ad_requests) ad_requests_fr,
        sum(programmatic_impressions) programmatic_impressions_fr,
        SAFE_DIVIDE(SUM(revenue), SUM(ad_requests))*1000 cpma_fr,
        SAFE_DIVIDE(sum(coalesce(programmatic_impressions, 0)), sum(COALESCE(ad_requests, 0))) fill_rate_fr,
        SAFE_DIVIDE(sum(floor_price * ad_requests), sum(ad_requests)) floor_price_fr

    from `sublime-elixir-273810.training_fill_rate.base_data_for_performance_checking`

    where time_day >= '{start_date}'
        and ad_unit_name = '{ad_unit}'
        {and_where}
    group by 1
),

rev_max as (
    select time_day as date,
        SUM(revenue) revenue_rm,
        SUM(ad_requests) ad_requests_rm,
        sum(programmatic_impressions) programmatic_impressions_rm,
        SAFE_DIVIDE(SUM(revenue), SUM(ad_requests))*1000 cpma_rm,
        SAFE_DIVIDE(sum(coalesce(programmatic_impressions, 0)), sum(COALESCE(ad_requests, 0))) fill_rate_rm,
        SAFE_DIVIDE(sum(floor_price * ad_requests), sum(ad_requests)) floor_price_rm

    from `sublime-elixir-273810.training_fill_rate.base_data_for_performance_checking`

    where time_day >= '{start_date}'
        and {reference_ad_units_where}
        {and_where}
    group by 1
)

select *
from fill_rate join rev_max using (date)
order by date