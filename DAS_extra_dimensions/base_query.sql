CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.<TABLE_NAME>` AS

with base as (
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
), results_opt_winning_rps as (
    select b.* except (status, revenue, session_count)
    from base b
    join results_expt r using (bidder, country_code, device_category, rtt_category, fsrefresh, status)
    where b.fs_testgroup = 'optimised' and b.session_count > 100
), results as (
    select o.*, e.status, e.rps, e.rps * o.session_count revenue, o_r.rps rps_opt, o_r.rps * o.session_count revenue_opt
    from results_expt e
    join results_opt o using (bidder, country_code, device_category, rtt_category, fsrefresh)
    left join results_opt_winning_rps o_r using (bidder, country_code, device_category, rtt_category, fsrefresh)
)
select * from results;

select count(*) unique_cohorts, sum(session_count) total_sessions, sum(revenue) revenue_no_domain, 0 revenue_domain
from `sublime-elixir-273810.ds_experiments_us.<TABLE_NAME>`;
