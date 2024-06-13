with base_with_extra_dim as (
    SELECT fs_testgroup, bidder, rtt_category, fsrefresh, country_code,
        `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category, <EXTRA_DIM>, status,
        sum(revenue) revenue,
        sum(session_count) session_count,
        safe_divide(sum(revenue), sum(session_count)) rps
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    WHERE DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_START> DAY) AND DATE <= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_END> DAY)
        and fs_testgroup in ('experiment', 'optimised')
        and country_code is not null
        and status != 'disabled'
        and ad_product not like '%video%'
    group by fs_testgroup, bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM>, status
), results_with_extra_dim_expt as (
    select *
    from base_with_extra_dim
    where fs_testgroup = 'experiment'
    qualify row_number() over (partition by bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM> order by rps desc) = 1
), results_with_extra_dim_opt as (
    select bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM>, sum(session_count) session_count
    from base_with_extra_dim
    where fs_testgroup = 'optimised'
    group by bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM>
), results_with_extra_dim as (
    select o.*, e.rps rps, e.rps * o.session_count revenue
    from results_with_extra_dim_expt e join results_with_extra_dim_opt o using (bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM>)
), base_without_extra_dim as (
    select base_with_extra_dim.*
    from base_with_extra_dim join results_with_extra_dim using (bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM>)
), base_without_extra_dim_expt as (
    select bidder, country_code, device_category, rtt_category, fsrefresh, status, safe_divide(sum(revenue), sum(session_count)) rps
    from base_without_extra_dim
    where fs_testgroup = 'experiment'
    group by bidder, country_code, device_category, rtt_category, fsrefresh, status
), results_without_extra_dim_expt as (
    select *
    from base_without_extra_dim_expt
    qualify row_number() over (partition by bidder, country_code, device_category, rtt_category, fsrefresh order by rps desc) = 1
), results_without_extra_dim_opt as (
    select bidder, country_code, device_category, rtt_category, fsrefresh, sum(session_count) session_count
    from base_without_extra_dim
    where fs_testgroup = 'optimised'
    group by bidder, country_code, device_category, rtt_category, fsrefresh
), results_without_extra_dim as (
    select o.*, e.rps rps, e.rps * o.session_count revenue
    from results_without_extra_dim_expt e join results_without_extra_dim_opt o using (bidder, country_code, device_category, rtt_category, fsrefresh)
), summary as (
    select 'with extra dimension' scenario, sum(session_count) session_count, sum(revenue) revenue, sum(revenue)/sum(session_count)*1000 rps
    from results_with_extra_dim
    union all
    select 'without extra dimension' scenario, sum(session_count) session_count, sum(revenue) revenue, sum(revenue)/sum(session_count)*1000 rps
    from results_without_extra_dim
)
select *, 100 * (revenue / (select revenue from summary where scenario = 'without extra dimension') - 1) as percent_increase_in_revenue
from summary
order by scenario desc
