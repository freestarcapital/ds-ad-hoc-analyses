
with fill_rate as (
    select time_day as date,
        SAFE_DIVIDE(SUM(revenue), SUM(ad_requests))*1000 cpma_fr,
        SAFE_DIVIDE(sum(coalesce(programmatic_impressions, 0)), sum(COALESCE(ad_requests, 0))) fill_rate_fr,
        SAFE_DIVIDE(sum(floor_price * ad_requests), sum(ad_requests)) floor_price_fr

    from `sublime-elixir-273810.training.base_data_main_green`
    where time_day >= '2025-6-1'
        and ad_unit_name = '{ad_unit}'
        {and_where}
    group by 1
),

rev_max as (
    select time_day as date,
        SAFE_DIVIDE(SUM(revenue), SUM(ad_requests))*1000 cpma_rm,
        SAFE_DIVIDE(sum(coalesce(programmatic_impressions, 0)), sum(COALESCE(ad_requests, 0))) fill_rate_rm,
        SAFE_DIVIDE(sum(floor_price * ad_requests), sum(ad_requests)) floor_price_rm

    from `sublime-elixir-273810.training.base_data_main_green`
    where time_day >= '2025-6-1'
        and ad_unit_name like '{ad_unit_domain_base_like}'
        and ad_unit_name != '{ad_unit}'
        {and_where}
    group by 1
)

select *
from fill_rate join rev_max using (date)
order by date