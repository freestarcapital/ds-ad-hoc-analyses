
with t1 as (
select bidder, status, domain, sum(session_count) session_count
from `streamamp-qa-239417.DAS_increment.daily_bidder_domain_expt_session_stats_join_2024-09-24_25_1`
where domain is not null and status in ('client')
and '2024-09-10' <= date and date <= '2024-09-11'
group by 1, 2, 3
order by 3,2,1
), t2 as (
select *
 from t1
qualify session_count >= 0.1 * avg(session_count) over(partition by domain)
)
select domain, count(*) bidders, sum(session_count) session_count
from t2
group by 1
