
with t1 as (
    select ad_unit_name
    from `streamamp-qa-239417.Floors_2_0.floors_ad_unit_base`
    group by 1
    order by SUM(requests) desc
    limit {ad_unit_count}
)

select
    ad_unit_name,
    cast(floor_price as FLOAT64) floor_price,
    SUM(requests) requests,
    COALESCE(SAFE_DIVIDE(SUM(impressions), SUM(requests)), 0) fill_rate,
    COALESCE(SAFE_DIVIDE(SUM(revenue), SUM(requests)), 0) * 1000 cpma,
    COALESCE(SAFE_DIVIDE(SUM(revenue), SUM(impressions)), 0) * 1000 cpm

from `streamamp-qa-239417.Floors_2_0.floors_ad_unit_base`
--join t1 using (ad_unit_name)
where optimised
and ad_unit_name in (select ad_unit_name from t1)


group by 1, 2
order by 1, 2


