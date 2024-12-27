
select
    floor_price,

    SUM(if(optimised, impressions, 0)) optimised_impressions,
    SUM(if(optimised, requests, 0)) optimised_requests,
    SUM(if(optimised, revenue, 0)) optimised_revenue,
    COALESCE(SAFE_DIVIDE(SUM(if(optimised, impressions, 0)), SUM(if(optimised, requests, 0))), 0) optimised_fill_rate,
    COALESCE(SAFE_DIVIDE(SUM(if(optimised, revenue, 0)), SUM(if(optimised, impressions, 0))), 0) * 1000 optimised_cpm,
    COALESCE(SAFE_DIVIDE(SUM(if(optimised, revenue, 0)), SUM(if(optimised, requests, 0))), 0) * 1000 optimised_cpma,

    SUM(if(baseline, impressions, 0)) baseline_impressions,
    SUM(if(baseline, requests, 0)) baseline_requests,
    SUM(if(baseline, revenue, 0)) baseline_revenue,
    COALESCE(SAFE_DIVIDE(SUM(if(baseline, impressions, 0)), SUM(if(baseline, requests, 0))), 0) baseline_fill_rate,
    COALESCE(SAFE_DIVIDE(SUM(if(baseline, revenue, 0)), SUM(if(baseline, impressions, 0))), 0) * 1000 baseline_cpm,
    COALESCE(SAFE_DIVIDE(SUM(if(baseline, revenue, 0)), SUM(if(baseline, requests, 0))), 0) * 1000 baseline_cpma,

from `streamamp-qa-239417.Floors_2_0.floors_ad_unit_base`
where {ad_unit_name} --ad_unit_name = "/15184186/signupgenius_Desktop_SignUps_Sheet_300x600_Right"
group by 1
order by 1


