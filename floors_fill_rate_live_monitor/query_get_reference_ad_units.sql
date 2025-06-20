select ad_unit_name, SUM(revenue) revenue, SUM(ad_requests) ad_requests
from `sublime-elixir-273810.training.base_data_main_green`
where time_day >= '2025-6-1'
    and {reference_ad_units_where}
    {and_where}
group by 1
