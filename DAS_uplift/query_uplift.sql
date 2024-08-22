CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.das_uplift_{tablename}_{processing_date}_{days_back_start}_{days_back_end}_{minimum_session_count}` AS

with base as (
    SELECT country_code, rtt_category, domain, ad_product,
        `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category, fsrefresh, fs_testgroup,
        session_count, revenue,
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    WHERE DATE_SUB('{processing_date}', INTERVAL {days_back_start} DAY) <= date AND date <= DATE_SUB('{processing_date}', INTERVAL {days_back_end} DAY)
        and fs_testgroup in ('experiment', 'optimised')
        and country_code is not null
        and status != 'disabled'
        and ad_product not like '%video%'
        and ad_product is not null
        and domain is not null
        and rtt_category is not null
        and fsrefresh is not null
        and `freestar-157323.ad_manager_dtf`.device_category(device_category) is not null
    ),

agg_expt as (
    SELECT {dims},
        sum(revenue) revenue, sum(session_count) session_count,
        coalesce(safe_divide(sum(revenue), sum(session_count)), 0) * 1000 rps
    from base
    where fs_testgroup = 'experiment'
    group by {dims}
    having session_count > {minimum_session_count}
    ),

agg_opt as (
    SELECT {dims},
        sum(revenue) revenue, sum(session_count) session_count,
        coalesce(safe_divide(sum(revenue), sum(session_count)), 0) * 1000 rps
    from base
    where fs_testgroup = 'optimised'
    group by {dims}
    having session_count > {minimum_session_count}
    ),

results as (
    select {dims},
        agg_opt.revenue revenue_opt, agg_opt.session_count session_count_opt, agg_opt.rps rps_opt,
        agg_expt.revenue revenue_expt, agg_expt.session_count session_count_expt, agg_expt.rps rps_expt,
        100 * (safe_divide(agg_opt.rps, agg_expt.rps) - 1) as rps_uplift_ratio_perc
    from agg_expt
    join agg_opt using ({dims})
    where agg_expt.rps > 0
)

select * from results;

select *
from `sublime-elixir-273810.ds_experiments_us.das_uplift_{tablename}_{processing_date}_{days_back_start}_{days_back_end}_{minimum_session_count}`
order by rps_uplift_ratio_perc
