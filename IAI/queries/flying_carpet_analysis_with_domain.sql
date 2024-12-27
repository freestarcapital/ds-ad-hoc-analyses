with pgv as (
    select distinct
--        coalesce(net.reg_domain(page_url), 'unknown') domain,
        net.reg_domain(page_url) domain,
        --coalesce(page_url, 'unknown') domain,
        session_id
    from `freestar-157323.prod_eventstream.pagehits_raw`
    where _PARTITIONDATE >= '{start_date}'
        and net.reg_domain(page_url) is not null
),

t1 as (
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
    domain,
    count(*) sessions,
    sum(all_impressions + all_unfilled) all_requests,
    sum(flying_carpet_impressions + flying_carpet_unfilled) flying_carpet_requests,
    sum(all_revenue) all_revenue,
    sum(flying_carpet_revenue) flying_carpet_revenue,
    safe_divide(sum(all_revenue), count(*)) * 1000 all_rps,
    safe_divide(sum(flying_carpet_revenue), count(*)) * 1000 flying_carpet_rps

from qp
join t1 on fs_session_id=session_id
join pgv using (session_id)
{where_clause}

group by 1, 2, 3
order by 1, 2, 3