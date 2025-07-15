with config_ad as (
    select date, {dims}, fs_ad_product, bidder
    from `streamamp-qa-239417.DAS_2_0.DAS_config_geo_{tablename_dims_ad}_optimised_2025-07-11` t11
    inner join unnest(t11.bidders) bidder
    where array_length(bidders) = 10
), perf_ad as (
    select date, {dims}, fs_ad_product, avg(rps) rps_ad, avg(all_bidder_session_count) sessions_ad
    from config_ad
    inner join `streamamp-qa-239417.DAS_2_0.DAS_bidder_rps_geo_{tablename_dims_ad}_optimised_2025-07-11`
    using (date, {dims}, fs_ad_product, bidder)
    group by date, {dims}, fs_ad_product
    having count(*) = 10
), config_no_ad as (
    select date, {dims}, bidder
    from `streamamp-qa-239417.DAS_2_0.DAS_config_geo_{tablename_dims}_optimised_2025-07-11` t11
    inner join unnest(t11.bidders) bidder
    where array_length(bidders) = 10
), perf_no_ad as (
    select date, {dims}, fs_ad_product, avg(rps) rps_no_ad
    from config_no_ad
    inner join `streamamp-qa-239417.DAS_2_0.DAS_bidder_rps_geo_{tablename_dims_ad}_optimised_2025-07-11`
    using (date, {dims}, bidder)
    group by date, {dims}, fs_ad_product
    having count(*) = 10
), session_count_no_ad as (
    select date, {dims}, avg(all_bidder_session_count) sessions_no_ad
    from config_no_ad
    inner join  `streamamp-qa-239417.DAS_2_0.DAS_bidder_rps_geo_{tablename_dims}_optimised_2025-07-11`
    using (date, {dims}, bidder)
    group by date, {dims}
    having count(*) = 10
), combined as (
    select *, rps_ad/rps_no_ad-1 rps_uplift_ad
    from perf_ad
    inner join perf_no_ad
    using (date, {dims}, fs_ad_product)
    inner join session_count_no_ad
    using (date, {dims})
)
select date,
    count(*) cohorts,
    sum(sessions_no_ad) sessions_no_ad,
    sum(sessions_ad) sessions_ad,
    sum(rps_uplift_ad * sessions_ad) / sum(sessions_ad) rps_uplift_ad_weighted
from combined
group by 1
order by 1