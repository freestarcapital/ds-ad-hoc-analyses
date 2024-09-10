
with status_fix as (

    select * except (status),
        if(bidder in  ('ix', 'rise', 'appnexus', 'rubicon', 'triplelift', 'pubmatic') and date > '2024-08-28' and status = 'disabled', 'client', status) status
    from `{project_id}.DAS_increment
    .{tablename}`
    where bidder not in ('amazon', 'preGAMAuction', 'seedtag', 'justpremium', 'sonobi', 'insticator')

--), pre_agg as (
--
--    select date,
--        {dimensions},
--        sum(session_count) session_count,
--        sum(revenue) sum_revenue,
--        sum(revenue_sq) sum_revenue_sq
--    from status_fix
--    group by 1, {dimensions}

--), agg as (
--
--    select date, {dimensions},
--        sum(session_count) over(partition by {dimensions} order by date rows between {N_days_preceding} preceding and current row) session_count,
--        sum(sum_revenue) over(partition by {dimensions} order by date rows between {N_days_preceding} preceding and current row) sum_revenue,
--        sum(sum_revenue_sq) over(partition by {dimensions} order by date rows between {N_days_preceding} preceding and current row) sum_revenue_sq
--    from pre_agg
-- where date = '{date}'

), first_agg as (

    select {dimensions},
        sum(session_count) session_count,
        sum(revenue) sum_revenue,
        sum(revenue_sq) sum_revenue_sq
    from status_fix
    where date = '{date}'
    group by {dimensions}

), country_code_default as (

    SELECT * except (country_code),
        CASE WHEN min(session_count) over(partition by {dimensions_without_country_code}) >= {min_session_count} THEN country_code ELSE 'default' END country_code,
    from first_agg

), second_agg as (

    SELECT * except (session_count, revenue, revenue_sq),
        sum(revenue) revenue, sum(session_count) session_count,sum(ad_requests) ad_requests
    from country_code_default
    group by {dimensions}

), pre_stats as (

    select {dimensions},
        session_count,
        safe_divide(sum_revenue, session_count) mean_revenue,
        safe_divide(sum_revenue_sq, session_count) mean_revenue_sq
    from second_agg

), stats as (

    select date, {dimensions},
        session_count,
        mean_revenue * 1000 rps,
        if(mean_revenue_sq < pow(mean_revenue, 2), 0, sqrt((mean_revenue_sq - pow(mean_revenue, 2)) / session_count)) * 1000 rps_std
    from pre_stats

)

select *
from stats

