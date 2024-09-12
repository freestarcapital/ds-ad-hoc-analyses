

CREATE OR REPLACE TABLE `{project_id}.DAS_increment.{tablename_to}`
    OPTIONS (
        expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
    AS

with status_fix as (

    select * --except (status),
        --if(bidder in  ('ix', 'rise', 'appnexus', 'rubicon', 'triplelift', 'pubmatic') and date > '2024-08-28' and status = 'disabled', 'client', status) status
        -- don't need the above as it's fixed in the data load query
    from `{project_id}.DAS_increment.{tablename_from}`
    where bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi', 'insticator')
        and status in ('client', 'server')
        and date_sub('{processing_date}', interval {days_back_start} day) <= date
            and date <= date_sub('{processing_date}', interval {days_back_end} day)
        and country_code is not null and country_code != ''

) , first_agg as (

    select bidder, status, country_code,
        sum(session_count) session_count,
        sum(revenue) revenue,
        sum(revenue_sq) revenue_sq
    from status_fix
    group by bidder, status, country_code

), country_code_agg as (
    select * except (country_code),
        if(sum(session_count) over(partition by country_code) > {min_all_bidder_session_count}, country_code, 'default') country_code
    from first_agg

), second_agg as (

    select bidder, status, country_code,
        sum(session_count) session_count,
        sum(revenue) revenue,
        sum(revenue_sq) revenue_sq
    from country_code_agg
    group by bidder, status, country_code
    having session_count > {min_individual_bidder_session_count}

), pre_stats as (

    select bidder, status, country_code,
        session_count,
        safe_divide(revenue, session_count) mean_revenue,
        safe_divide(revenue_sq, session_count) mean_revenue_sq
    from second_agg

), stats as (

    select bidder, status, country_code,
        session_count,
        mean_revenue * 1000 rps,
        if(mean_revenue_sq < pow(mean_revenue, 2), 0, sqrt((mean_revenue_sq - pow(mean_revenue, 2)) / session_count)) * 1000 rps_std
    from pre_stats

), rank as (

    select *, safe_divide(rps, rps_std) rps_z_score,
        row_number() over(partition by country_code order by rps desc) rn
    from stats

)

select cast('{processing_date}' as date) as date, *
from rank

