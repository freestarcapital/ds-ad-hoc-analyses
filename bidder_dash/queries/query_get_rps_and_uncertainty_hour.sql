
with status_fix as (

    select * except (status),
        if(bidder in  ('ix', 'rise', 'appnexus', 'rubicon', 'triplelift', 'pubmatic') and date_hour > '2024-08-28 20:00:00' and status = 'disabled', 'client', status) status
    from `{project_id}.DAS_eventstream_session_data.{tablename}`

), agg as (

    select date_hour, {select_dimensions},
        sum(session_count) session_count,
        sum(revenue) sum_revenue,
        sum(revenue_sq) sum_revenue_sq
    from status_fix
    {where}
    group by date_hour, {group_by_dimensions}

), rolling_agg as (

    select date_hour, {group_by_dimensions},
        sum(session_count) over(partition by {group_by_dimensions} order by date_hour rows between {N_hours_preceding} preceding and current row) session_count,
        sum(sum_revenue) over(partition by {group_by_dimensions} order by date_hour rows between {N_hours_preceding} preceding and current row) sum_revenue,
        sum(sum_revenue_sq) over(partition by {group_by_dimensions} order by date_hour rows between {N_hours_preceding} preceding and current row) sum_revenue_sq
    from agg

), stats as (

    select date_hour, {group_by_dimensions},
        session_count,
        safe_divide(sum_revenue, session_count) mean_revenue,
        safe_divide(sum_revenue_sq, session_count) mean_revenue_sq
    from rolling_agg

)

select date_hour, {group_by_dimensions},
    session_count,
    mean_revenue * 1000 rps,
    sqrt((mean_revenue_sq - pow(mean_revenue, 2)) / session_count) * 1000 rps_std

from stats


