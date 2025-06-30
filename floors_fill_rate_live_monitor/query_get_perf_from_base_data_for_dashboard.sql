{create_or_insert_statement}

with fill_rate as (
    select time_day as date, country_code, device_category,
        SUM(revenue) sum_revenue_fr,
        SUM(ad_requests) sum_ad_requests_fr,
        sum(programmatic_impressions) sum_programmatic_impressions_fr,
        sum(floor_price * ad_requests) sum_floor_price_ad_requests_fr

    from `sublime-elixir-273810.training_fill_rate.base_data_for_performance_checking`

    where time_day >= '{start_date}'
        and ad_unit_name = '{ad_unit}'
    group by 1, 2, 3
),

rev_max as (
    select time_day as date, country_code, device_category,
        SUM(revenue) sum_revenue_rm,
        SUM(ad_requests) sum_ad_requests_rm,
        sum(programmatic_impressions) sum_programmatic_impressions_rm,
        sum(floor_price * ad_requests) sum_floor_price_ad_requests_rm

    from `sublime-elixir-273810.training_fill_rate.base_data_for_performance_checking`

    where time_day >= '{start_date}'
        and {reference_ad_units_where}
    group by 1, 2, 3
)

select '{ad_unit}' ad_unit, *,
    safe_divide(sum_revenue_rm, sum_programmatic_impressions_rm) * 1000 cpm_rm,
    safe_divide(sum_revenue_rm, sum_ad_requests_rm) * 1000 cpma_rm,
    safe_divide(sum_programmatic_impressions_rm, sum_ad_requests_rm) fill_rate_rm,
    safe_divide(sum_floor_price_ad_requests_rm, sum_ad_requests_rm) ad_request_weighted_floor_price_rm,
    safe_divide(sum_revenue_fr, sum_programmatic_impressions_fr) * 1000 cpm_fr,
    safe_divide(sum_revenue_fr, sum_ad_requests_fr) * 1000 cpma_fr,
    safe_divide(sum_programmatic_impressions_fr, sum_ad_requests_fr) fill_rate_fr,
    safe_divide(sum_floor_price_ad_requests_fr, sum_ad_requests_fr) ad_request_weighted_floor_price_fr,
    if(date >= '{fill_rate_model_enabled_date}', 1, 0) fill_rate_model_enabled,
    '{fill_rate_model_enabled_date}' fill_rate_model_enabled_date
from fill_rate join rev_max using (date, country_code, device_category)
order by date

-- for looker

--         cpma: SUM(revenue) / SUM(ad_requests) * 1000
--         fill_rate: sum(programmatic_impressions) / sum(ad_requests)
--         ad_request_weighted_floor_price: sum(sum_floor_price_ad_requests) / sum(ad_requests)
