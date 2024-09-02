with agg as (

    select {dimensions}, count(*) session_count,
        safe_divide(sum(revenue_sq), count(*)) mean_revenue_sq,
        safe_divide(sum(revenue), count(*)) mean_revenue

    from `{project_id}.DAS_eventstream_session_data.{tablename}`
    {where}
    group by {dimensions}

)

select {dimensions},
    session_count,
    mean_revenue * 1000 rps,
    sqrt((mean_revenue_sq - pow(mean_revenue, 2)) / session_count) * 1000 rps_std
from agg

