{create_or_insert_statement}

with t2 as (
    select distinct
        (SELECT REGEXP_EXTRACT(kvps, "fs_testgroup=(.*)") FROM UNNEST(t1.kvps) kvps WHERE kvps LIKE "%fs_testgroup=%" LIMIT 1) AS fs_testgroup,
        auction_id
    from freestar-157323.prod_eventstream.auction_start_raw t1
    where _PARTITIONDATE = '{ddate}'
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t1.kvps) kvpss
            WHERE
                kvpss in ("fs_testgroup=optimised", "fs_testgroup=optimised-static-timeout-3002")
        ) = 1
        AND (
            SELECT COUNT(1)
            FROM UNNEST(t1.kvps) kvpss
            WHERE
                (kvpss like 'fsrefresh=%') and not (kvpss = "fsrefresh=0")
        ) = 1
),

t3 as (
    select *
    from t2
    qualify count(*) over(partition by auction_id) = 1
),

t4 as (
    select auction_id, count(*) requests_per_auction
    from freestar-157323.prod_eventstream.bidsrequest_raw
    where _PARTITIONDATE = '{ddate}'
    group by 1
),

t5 as (
select auction_id, countif(time_to_respond < auction_timeout) in_time_responses_per_auction, count(*) total_responses_per_auction
  from freestar-157323.prod_eventstream.bidsresponse_raw
  where _PARTITIONDATE = '{ddate}' and status_message = 'Bid available'
  group by 1
),

t6 as (
select auction_id, sum(cpm / 1e7) as revenue
  from freestar-157323.prod_eventstream.bidswon_raw
  where _PARTITIONDATE = '{ddate}'
  group by 1
),

t7 as (
    select fs_testgroup,
        count(*) auction_start_count,
        sum(requests_per_auction) request_count,
        avg(requests_per_auction) avg_requests_per_auction,
        avg(in_time_responses_per_auction) avg_in_time_responses_per_auction,
        avg(total_responses_per_auction) avg_total_responses_per_auction,
        sum(revenue) revenue,
        safe_divide(sum(revenue), count(*)) * 1000 revenue_per_1000_auctions,
        safe_divide(sum(revenue), sum(requests_per_auction)) * 1000 revenue_per_1000_requests
    from t3
    left join t4 using (auction_id)
    left join t5 using (auction_id)
    left join t6 using (auction_id)
    group by 1
)

select '{ddate}' date, *
from t7;

-- ANALYSE THE RESULTS
-- with t1 as (
--   select *
--   from streamamp-qa-239417.DAS_increment.FI_timeouts_performance_results_detailed
--   where fs_testgroup='optimised'
-- ),
-- t2 as (
--   select *
--   from streamamp-qa-239417.DAS_increment.FI_timeouts_performance_results_detailed
--   where fs_testgroup='optimised-static-timeout-3002'
-- ),
-- t3 as(
-- select date,
--     100*(t1.revenue_per_1000_auctions / t2.revenue_per_1000_auctions-1) revenue_per_1000_auctions_uplift,
--     100*(t1.revenue_per_1000_requests / t2.revenue_per_1000_requests-1) revenue_per_1000_requests_uplift
-- from t1 join t2 using (date)
-- )
-- select avg(revenue_per_1000_auctions_uplift), stddev(revenue_per_1000_auctions_uplift)
-- from t3
-- where date >= '2025-09-01'
-- order by 1
