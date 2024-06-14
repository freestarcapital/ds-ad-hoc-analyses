with base_raw_country_code as (
    SELECT fs_testgroup, bidder, rtt_category, fsrefresh,
        country_code,
        `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category, status,
        sum(revenue) revenue,
        sum(session_count) session_count,
        safe_divide(sum(revenue), sum(session_count)) rps
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    WHERE DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_START> DAY) AND DATE <= DATE_SUB(CURRENT_DATE(), INTERVAL <DAYS_BACK_END> DAY)
        and fs_testgroup in ('experiment', 'optimised')
        and country_code is not null
        and status != 'disabled'
        and ad_product not like '%video%'
    group by fs_testgroup, bidder, country_code, device_category, rtt_category, fsrefresh, status
), country_code_criteria as (
    select bidder, rtt_category, fsrefresh, country_code, device_category
        from base_raw_country_code
        where fs_testgroup = 'experiment'
        group by bidder, rtt_category, fsrefresh, country_code, device_category
        having min(session_count) >= 100
), base as (
    select b.fs_testgroup, b.bidder, b.rtt_category, b.fsrefresh, b.device_category, b.status,
        coalesce(cc.country_code, 'default') country_code,
        sum(revenue) revenue, sum(session_count) session_count, safe_divide(sum(revenue), sum(session_count)) rps
    from base_raw_country_code b
    left join country_code_criteria cc using (bidder, rtt_category, fsrefresh, country_code, device_category)
    group by fs_testgroup, bidder, country_code, device_category, rtt_category, fsrefresh, status
), results_expt as (
    select *
    from base
    where fs_testgroup = 'experiment'
    qualify row_number() over (partition by bidder, country_code, device_category, rtt_category, fsrefresh order by rps desc) = 1
), results_opt as (
    select bidder, country_code, device_category, rtt_category, fsrefresh, sum(session_count) session_count
    from base
    where fs_testgroup = 'optimised'
    group by bidder, country_code, device_category, rtt_category, fsrefresh
), results as (
    select o.*, e.rps rps, e.rps * o.session_count revenue
    from results_expt e join results_opt o using (bidder, country_code, device_category, rtt_category, fsrefresh)
)
select count(*) unique_cohorts, sum(session_count) total_sessions, sum(revenue) revenue_no_domain, 0 revenue_domain
from results
