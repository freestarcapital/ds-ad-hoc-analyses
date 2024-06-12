CREATE OR REPLACE TABLE `sublime-elixir-273810.ds_experiments_us.das_extra_dim_base_data_<EXTRA_DIM>_<COUNTRY_CODE_NAME>_<FS_TESTGROUP>` AS

with base as (
    SELECT date, bidder, country_code, rtt_category, fsrefresh,
        `freestar-157323.ad_manager_dtf`.device_category(device_category) device_category,
        <EXTRA_DIM>,
        status, impressions, unfilled, session_count, revenue
    FROM `freestar-157323.ad_manager_dtf.daily_client_server_mask_reporting_refresh_w_domain_expanded`
    WHERE DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY) AND DATE <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        and fs_testgroup = '<FS_TESTGROUP>'
        and country_code is not null
        and status != 'disabled'
        and ad_product not like '%video%'
),

aggregated as (
    SELECT bidder,
        <COUNTRY_CODE_QUERY> country_code,
        device_category, rtt_category, fsrefresh, <EXTRA_DIM>, status,
    sum(revenue) revenue, sum(session_count) session_count
    from base
    group by bidder, country_code, device_category, rtt_category, fsrefresh, <EXTRA_DIM>, status
),

top_cohorts as(
    select *
    from aggregated
    qualify row_number() over (partition by country_code, device_category, bidder, fsrefresh, rtt_category, <EXTRA_DIM>
        order by safe_divide(revenue, session_count) desc) = 1
)

select * from top_cohorts