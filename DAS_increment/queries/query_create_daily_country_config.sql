

CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with raw as (

    select *
    from `{project_id}.DAS_increment.{tablename_from}`
    left join `sublime-elixir-273810.ideal_ad_stack.continent_country_mapping` on country_code = geo_country
    where bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi')
        and status in ('client', 'server')
       -- and date_sub('{processing_date}', interval {days_back_start} day) <= date
            --and date <= date_sub('{processing_date}', interval {days_back_end} day)
        and date <= date_sub('{processing_date}', interval 1 day)
        and country_code is not null and country_code != ''

), agg_1_day as (
    select date, bidder, status {dims},
        sum(session_count) session_count,
        sum(revenue) revenue,
        sum(revenue_sq) revenue_sq
    from raw
    group by date, bidder, status {dims}


), agg_N_days as (

    select date, bidder, status {dims},
        sum(session_count) over(partition by bidder, status {dims} order by date rows between {N_days_preceding} preceding and current row) session_count,
        sum(revenue) over(partition by bidder, status {dims} order by date rows between {N_days_preceding} preceding and current row) revenue,
        sum(revenue_sq) over(partition by bidder, status {dims} order by date rows between {N_days_preceding} preceding and current row) revenue_sq
    from agg_1_day

), qual_1 as (

    select *
    from agg_N_days
    where session_count > {min_individual_bidder_session_count}
        and date >= date_add((select min(date) from `{project_id}.DAS_increment.{tablename_from}`), interval {N_days_preceding} day)

), qual_2 as (
    select *
    from qual_1
    qualify sum(session_count) over(partition by date, status {dims}) > {min_all_bidder_session_count}

), pre_stats as (

    select date, bidder, status {dims},
        session_count,
        safe_divide(revenue, session_count) mean_revenue,
        safe_divide(revenue_sq, session_count) mean_revenue_sq
    from qual_2

), stats as (

    select date, bidder, status {dims},
        session_count,
        mean_revenue * 1000 rps,
        if(mean_revenue_sq < pow(mean_revenue, 2), 0, sqrt((mean_revenue_sq - pow(mean_revenue, 2)) / session_count)) * 1000 rps_std
    from pre_stats

), rank as (

    select *, ifnull(safe_divide(rps, rps_std), 0) rps_z_score,
        row_number() over(partition by date, status {dims} order by rps desc) rn
    from stats

)

select cast('{processing_date}' as date) as processing_date, *
from rank



