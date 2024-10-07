

CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with status_fix as (

    select *
    from `{project_id}.DAS_increment.{tablename_from}`
    where bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi')
        and status in ('client', 'server')
        and date_sub('{processing_date}', interval {days_back_start} day) <= date
            and date <= date_sub('{processing_date}', interval {days_back_end} day)
        and country_code is not null and country_code != ''

) , first_agg as (

    select bidder, status, {dims},
        sum(session_count) session_count,
        sum(revenue) revenue,
        sum(revenue_sq) revenue_sq
    from status_fix
    group by bidder, status, {dims}

), country_code_agg as (
    select * except (country_code),
        if(sum(session_count) over(partition by status, {dims}) > {min_all_bidder_session_count}, country_code, 'default') country_code
    from first_agg

), second_agg as (

    select bidder, status, {dims},
        sum(session_count) session_count,
        sum(revenue) revenue,
        sum(revenue_sq) revenue_sq
    from country_code_agg
    group by bidder, status, {dims}
    having session_count > {min_individual_bidder_session_count}

), pre_stats as (

    select bidder, status, {dims},
        session_count,
        safe_divide(revenue, session_count) mean_revenue,
        safe_divide(revenue_sq, session_count) mean_revenue_sq
    from second_agg

), stats as (

    select bidder, status, {dims},
        session_count,
        mean_revenue * 1000 rps,
        if(mean_revenue_sq < pow(mean_revenue, 2), 0, sqrt((mean_revenue_sq - pow(mean_revenue, 2)) / session_count)) * 1000 rps_std
    from pre_stats

), rank as (

    select *, ifnull(safe_divide(rps, rps_std), 0) rps_z_score,
        row_number() over(partition by {dims} order by rps desc) rn
    from stats

)

select cast('{processing_date}' as date) as date, *
from rank



