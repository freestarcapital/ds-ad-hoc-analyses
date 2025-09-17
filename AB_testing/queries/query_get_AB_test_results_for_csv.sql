with alias_cols as (
    select *,
        asr_requests requests_auction_start,
        bwr_native_render_revenue prebid_native_render_revenue,
        bwr_native_render_impressions prebid_native_render_impressions,
        prebid_revenue - bwr_native_render_revenue prebid_non_native_render_revenue,
        prebid_impressions - bwr_native_render_impressions prebid_non_native_render_impressions,
    from streamamp-qa-239417.DAS_increment.BI_AB_test_page_hits_results_transparent_floors_2
    where test_name_str != 'null' and sessions >= 10000
    qualify (count(*) over (partition by date, domain, test_name_str) = 2)
        and (safe_divide(sum(sessions_gam_data) over (partition by domain, date), sum(sessions) over (partition by domain, date)) > 0.5)
)
select
    date, domain, test_name_str test_name, test_group,
    sessions,
    safe_divide(sessions_asr_data, sessions) session_prop_asr_data,
    safe_divide(sessions_aer_data, sessions) session_prop_aer_data,
    safe_divide(sessions_bwr_data, sessions) session_prop_bwr_data,
    safe_divide(sessions_gam_data, sessions) session_prop_gam_data,
    requests, requests_auction_start,
    revenue, prebid_revenue, prebid_native_render_revenue, prebid_non_native_render_revenue,
    safe_divide(revenue, sessions) * 1000 revenue_per_session,
    safe_divide(prebid_revenue, sessions) * 1000 prebid_revenue_per_session,
    safe_divide(prebid_native_render_revenue, sessions) * 1000 prebid_native_render_revenue_per_session,
    safe_divide(prebid_non_native_render_revenue, sessions) * 1000 prebid_non_native_render_revenue_per_session,
    safe_divide(prebid_revenue, revenue) prebid_share_of_revenue,
    safe_divide(prebid_native_render_revenue, revenue) prebid_native_render_share_of_revenue,
    safe_divide(prebid_non_native_render_revenue, revenue) prebid_non_native_render_share_of_revenue,
    impressions, prebid_impressions, prebid_native_render_impressions, prebid_non_native_render_impressions,
    safe_divide(impressions, sessions) impressions_per_session,
    safe_divide(prebid_impressions, sessions) prebid_impressions_per_session,
    safe_divide(prebid_native_render_impressions, sessions) prebid_native_render_impressions_per_session,
    safe_divide(prebid_non_native_render_impressions, sessions) prebid_non_native_render_impressions_per_session,
    safe_divide(prebid_impressions, impressions) prebid_share_of_impressions,
    safe_divide(prebid_native_render_impressions, impressions) prebid_native_render_share_of_impressions,
    safe_divide(prebid_non_native_render_impressions, impressions) prebid_non_native_render_share_of_impressions,
    unfilled,
    safe_divide(revenue, impressions) * 1000 CPM,
    safe_divide(prebid_revenue, prebid_impressions) * 1000 prebid_CPM,
    safe_divide(prebid_native_render_revenue, prebid_native_render_impressions) * 1000 prebid_native_render_CPM,
    safe_divide(prebid_non_native_render_revenue, prebid_non_native_render_impressions) * 1000 prebid_non_native_render_CPM,
    safe_divide(impressions, requests) fill_rate,
    safe_divide(impressions - prebid_native_render_impressions, requests) fill_rate_excluding_prebid_native_render,
    safe_divide(revenue, requests) * 1000 CPMA,
    safe_divide(revenue, requests_auction_start) * 1000 CPMA_auction_start
from alias_cols

order by domain, date, test_name, test_group