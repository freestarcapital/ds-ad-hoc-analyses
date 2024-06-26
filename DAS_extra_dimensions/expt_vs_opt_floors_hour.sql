with base as (
    SELECT floors_hour, bidder, rtt_category, fsrefresh,
        country_code,
        `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category, status,
        sum(if(fs_testgroup='experiment', session_count, 0)) session_count_expt,
        sum(if(fs_testgroup='optimised', session_count, 0)) session_count_opt,
        coalesce(safe_divide(sum(if(fs_testgroup='experiment', revenue, 0)), sum(if(fs_testgroup='experiment', session_count, 0))), 0) rps_expt,
        coalesce(safe_divide(sum(if(fs_testgroup='optimised', revenue, 0)), sum(if(fs_testgroup='optimised', session_count, 0))), 0) rps_opt
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    WHERE DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND DATE <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        and fs_testgroup in ('experiment', 'optimised')
        and country_code is not null
        and status != 'disabled'
        and ad_product not like '%video%'
    group by floors_hour, bidder, country_code, device_category, rtt_category, fsrefresh, status
)
select floors_hour, (rps_opt - rps_expt)/(0.5*(rps_opt + rps_expt)) * 100 from base
where session_count_expt > 100 and session_count_opt > 100 and rps_opt > 0 and rps_expt > 0