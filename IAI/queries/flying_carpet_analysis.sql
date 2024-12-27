with t1 as (
    select
        fs_session_id, date,
        sum(all_revenue) all_revenue, sum(all_impressions) all_impressions, sum(all_unfilled) all_unfilled,
        sum(flying_carpet_revenue) flying_carpet_revenue, sum(flying_carpet_impressions) flying_carpet_impressions,
        sum(flying_carpet_unfilled) flying_carpet_unfilled
    from `streamamp-qa-239417.DAS_increment.IAI_dtf_session_data_new_all_data_{start_date}_{end_date}`
    group by 1, 2
), 

qp as (
    select session_id,
        floor(percentile_placement) quantile_placement,
        count(*) over (partition by session_id) quantile_placement_count_per_session
    from `freestar-157323.prod_eventstream.iai_high_impact_tracking`
    where _PARTITIONDATE >= '{start_date}'
        and percentile_placement > 1
)

select
    quantile_placement,
    date,
    count(*) sessions,
    sum(all_impressions + all_unfilled) all_requests,
    sum(flying_carpet_impressions + flying_carpet_unfilled) flying_carpet_requests,
    sum(all_revenue) all_revenue, sum(all_impressions) all_impressions, sum(all_unfilled) all_unfilled,
    sum(flying_carpet_revenue) flying_carpet_revenue, sum(flying_carpet_impressions) flying_carpet_impressions, sum(flying_carpet_unfilled) flying_carpet_unfilled,
    sum(all_revenue) / count(*) * 1000 all_rps,
    sum(flying_carpet_revenue) / count(*) * 1000 flying_carpet_rps,
    sum(all_revenue) / sum(all_impressions+all_unfilled) * 1000 all_cpma,
    sum(flying_carpet_revenue) / sum(flying_carpet_impressions+flying_carpet_unfilled) * 1000 flying_carpet_cpma,
    sum(all_impressions) / sum(all_impressions+all_unfilled) all_fill_rate,
    sum(flying_carpet_impressions) / sum(flying_carpet_impressions+flying_carpet_unfilled) flying_carpet_fill_rate

from qp
join t1
on fs_session_id=session_id
{where_clause}

group by 1, 2
order by 1, 2