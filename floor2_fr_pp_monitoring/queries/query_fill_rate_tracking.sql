select date_hour,
    date(date_hour) date,
    extract(hour from date_hour) hour,
    floor_price, optimised_cpma, optimised_fill_rate, price__pressure price_pressure
from `streamamp-qa-239417.Floors_2_0.floors_ad_unit_dash`
where ad_unit_name = '{ad_unit_name}'

order by date_hour